// src/platform/direct_file.cpp - see include/strata/platform/direct_file.hpp.
#include "strata/platform/direct_file.hpp"
#include "strata/platform/aux_cpus.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>
#if defined(__linux__) && __has_include(<linux/io_uring.h>)
#include <linux/io_uring.h>
#define STRATA_HAVE_LINUX_IO_URING 1
#endif
#endif

namespace strata::platform {

double now_us() {
    using namespace std::chrono;
    return (double) duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count() / 1000.0;
}

void* DirectFile::alloc_aligned(size_t bytes) {
#if defined(_WIN32)
    return VirtualAlloc(nullptr, bytes, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
#else
    void* p = nullptr;
    return posix_memalign(&p, alignment(), bytes) == 0 ? p : nullptr;
#endif
}

void DirectFile::free_aligned(void* p) {
    if (p == nullptr) return;
#if defined(_WIN32)
    VirtualFree(p, 0, MEM_RELEASE);
#else
    std::free(p);
#endif
}

namespace {
/// A queued read: `submit` only queues it; the pool's threads issue it (perf-review C-4 / F-2).  One thread
/// issuing every read was the limit of the n-gram table's prompt reads: ~12 us of kernel time per read, ~80K
/// reads/s, while an NVMe drive serves several times that at depth.
struct Pending {
    uint64_t offset;
    void* buffer;
    uint32_t length;
    uint64_t tag;
};

/// The number of issuing threads: STRATA_IO_THREADS, else 4 (Windows: overlapped submits) / 16 (Linux: each
/// thread does one blocking pread, so the thread count is the queue depth).
int io_threads(int dflt) {
    const char* v = std::getenv("STRATA_IO_THREADS");
    const int n = v ? std::atoi(v) : dflt;
    return std::clamp(n, 1, 64);
}
}  // namespace

#if defined(_WIN32)
// ------------------------------------------------------------------------------------------------ Windows
namespace {
/// One in-flight request. The OVERLAPPED must be the first member so a completion packet's OVERLAPPED*
/// converts back to the request.
struct Req {
    OVERLAPPED ov;
    uint64_t tag;
};
}  // namespace

struct DirectFile::Impl {
    HANDLE file = INVALID_HANDLE_VALUE;
    HANDLE port = nullptr;
    uint64_t size = 0;
    std::mutex mu;                      // everything below
    std::deque<Req*> free_reqs;
    std::vector<Req*> all_reqs;
    std::deque<Completion> immediate;   // requests that completed without a port packet (EOF) or failed
    std::deque<Pending> queue;          // submitted, not yet issued
    std::condition_variable cv;
    bool stop = false;
    std::vector<std::thread> pool;

    Req* take() {
        if (free_reqs.empty()) {
            Req* r = new Req();
            all_reqs.push_back(r);
            return r;
        }
        Req* r = free_reqs.front();
        free_reqs.pop_front();
        return r;
    }

    void issue(const Pending& p) {
        Req* r;
        {
            std::lock_guard<std::mutex> lk(mu);
            r = take();
        }
        std::memset(&r->ov, 0, sizeof r->ov);
        r->ov.Offset = (DWORD) (p.offset & 0xFFFFFFFFull);
        r->ov.OffsetHigh = (DWORD) (p.offset >> 32);
        r->tag = p.tag;
        if (!ReadFile(file, p.buffer, p.length, nullptr, &r->ov)) {
            const DWORD e = GetLastError();
            if (e != ERROR_IO_PENDING) {
                std::lock_guard<std::mutex> lk(mu);
                free_reqs.push_back(r);
                // at or past end of file: a zero-byte completion; anything else: a failed one
                immediate.push_back(Completion{p.tag, 0, e == ERROR_HANDLE_EOF});
                PostQueuedCompletionStatus(port, 0, 0, nullptr);   // wake a waiter to collect it
            }
        }
    }

    void worker() {
        std::unique_lock<std::mutex> lk(mu);
        for (;;) {
            cv.wait(lk, [&] { return stop || !queue.empty(); });
            if (stop && queue.empty()) return;
            const Pending p = queue.front();
            queue.pop_front();
            lk.unlock();
            issue(p);
            lk.lock();
        }
    }

    void stop_pool() {
        {
            std::lock_guard<std::mutex> lk(mu);
            stop = true;
        }
        cv.notify_all();
        for (std::thread& t : pool) t.join();
        pool.clear();
        stop = false;
    }
};

DirectFile::DirectFile() : impl_(new Impl) {}
DirectFile::~DirectFile() {
    close();
    for (Req* r : impl_->all_reqs) delete r;
    delete impl_;
}

bool DirectFile::open(const std::string& path, std::string& err) {
    close();
    const int wlen = MultiByteToWideChar(CP_UTF8, 0, path.c_str(), -1, nullptr, 0);
    std::wstring wpath((size_t) (wlen > 0 ? wlen : 1), L'\0');
    MultiByteToWideChar(CP_UTF8, 0, path.c_str(), -1, wpath.data(), wlen);
    // NO_BUFFERING: the cache manager never holds these pages, so the table cannot grow into RAM.
    // RANDOM_ACCESS: no read-ahead. OVERLAPPED: many reads in flight, completed through the port below.
    impl_->file = CreateFileW(wpath.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING,
                              FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED | FILE_FLAG_RANDOM_ACCESS, nullptr);
    if (impl_->file == INVALID_HANDLE_VALUE) {
        err = "DirectFile: cannot open " + path + " (error " + std::to_string(GetLastError()) + ")";
        return false;
    }
    LARGE_INTEGER sz;
    if (!GetFileSizeEx(impl_->file, &sz)) {
        err = "DirectFile: cannot size " + path;
        close();
        return false;
    }
    impl_->size = (uint64_t) sz.QuadPart;
    impl_->port = CreateIoCompletionPort(impl_->file, nullptr, 0, 1);
    if (impl_->port == nullptr) {
        err = "DirectFile: CreateIoCompletionPort failed (error " + std::to_string(GetLastError()) + ")";
        close();
        return false;
    }
    // Completions of reads that finish synchronously are still queued to the port, so every issued read
    // produces exactly one packet; `wait` is the only completion path.
    const int n = io_threads(4);
    for (int i = 0; i < n; ++i) impl_->pool.emplace_back([this] { strata::aux_cpus::pin_current_thread(); impl_->worker(); });
    return true;
}

void DirectFile::close() {
    impl_->stop_pool();
    if (impl_->port != nullptr) CloseHandle(impl_->port);
    if (impl_->file != INVALID_HANDLE_VALUE) CloseHandle(impl_->file);
    impl_->port = nullptr;
    impl_->file = INVALID_HANDLE_VALUE;
    impl_->size = 0;
    impl_->immediate.clear();
    impl_->queue.clear();
}

bool DirectFile::is_open() const { return impl_->file != INVALID_HANDLE_VALUE; }
uint64_t DirectFile::size() const { return impl_->size; }

bool DirectFile::submit(uint64_t offset, void* buffer, uint32_t length, uint64_t tag, std::string& err) {
    const ReadRequest req{offset, buffer, length, tag};
    return submit_batch(&req, 1, err);
}

bool DirectFile::submit_batch(const ReadRequest* reqs, size_t n, std::string& err) {
    if (!is_open()) { err = "DirectFile: not open"; return false; }
    for (size_t i = 0; i < n; ++i) {
        if (reqs[i].offset % alignment() || reqs[i].length % alignment() ||
            ((uintptr_t) reqs[i].buffer) % alignment() || reqs[i].length == 0) {
            err = "DirectFile: unaligned request";
            return false;
        }
    }
    if (n == 0) return true;
    {
        std::lock_guard<std::mutex> lk(impl_->mu);
        for (size_t i = 0; i < n; ++i) {
            impl_->queue.push_back(Pending{reqs[i].offset, reqs[i].buffer, reqs[i].length, reqs[i].tag});
        }
    }
    if (n == 1) impl_->cv.notify_one();
    else impl_->cv.notify_all();
    return true;
}

int DirectFile::wait(Completion* out, int max, int timeout_ms) {
    int n = 0;
    {
        std::lock_guard<std::mutex> lk(impl_->mu);
        while (n < max && !impl_->immediate.empty()) {
            out[n++] = impl_->immediate.front();
            impl_->immediate.pop_front();
        }
    }
    if (n == max || !is_open()) return n;
    OVERLAPPED_ENTRY entries[64];
    const ULONG want = (ULONG) (max - n < 64 ? max - n : 64);
    ULONG got = 0;
    const DWORD t = timeout_ms < 0 ? INFINITE : (DWORD) timeout_ms;
    if (!GetQueuedCompletionStatusEx(impl_->port, entries, want, &got, n > 0 ? 0 : t, FALSE)) return n;
    std::lock_guard<std::mutex> lk(impl_->mu);
    for (ULONG i = 0; i < got; ++i) {
        if (entries[i].lpOverlapped == nullptr) {          // a wake() packet (or an immediate completion's nudge)
            while (n < max && !impl_->immediate.empty()) {
                out[n++] = impl_->immediate.front();
                impl_->immediate.pop_front();
            }
            if (n < max) out[n++] = Completion{WAKE_TAG, 0, true};
            continue;
        }
        Req* r = (Req*) entries[i].lpOverlapped;
        const uint32_t status = (uint32_t) r->ov.Internal;   // an NTSTATUS
        Completion c;
        c.tag = r->tag;
        c.bytes = entries[i].dwNumberOfBytesTransferred;
        // STATUS_END_OF_FILE (0xC0000011) is a legal short read at the table's last page.
        c.ok = status == 0 || status == 0xC0000011u;
        out[n++] = c;
        impl_->free_reqs.push_back(r);
    }
    return n;
}

void DirectFile::wake() {
    if (impl_->port != nullptr) PostQueuedCompletionStatus(impl_->port, 0, 0, nullptr);
}

#else
// ------------------------------------------------------------------------------------------------ POSIX
namespace {

template <typename T>
struct Ring {
    std::vector<T> buf;
    size_t head = 0;
    size_t count = 0;

    void init(size_t cap = 256) {
        buf.resize(cap);
        head = 0;
        count = 0;
    }
    bool empty() const { return count == 0; }
    size_t size() const { return count; }
    void clear() {
        head = 0;
        count = 0;
    }
    void push(const T& v) {
        if (count == buf.size()) grow();
        buf[(head + count) & (buf.size() - 1)] = v;
        ++count;
    }
    T pop() {
        T v = buf[head];
        head = (head + 1) & (buf.size() - 1);
        --count;
        return v;
    }
    void grow() {
        const size_t old_cap = buf.size();
        const size_t new_cap = old_cap ? old_cap * 2 : 256;
        std::vector<T> next(new_cap);
        for (size_t i = 0; i < count; ++i) next[i] = buf[(head + i) & (old_cap - 1)];
        buf.swap(next);
        head = 0;
    }
};

#if defined(STRATA_HAVE_LINUX_IO_URING)
bool want_direct_uring() {
    const char* v = std::getenv("STRATA_DIRECT_URING");
    return v != nullptr && (v[0] == '1' || v[0] == 'y' || v[0] == 'Y' || v[0] == 't' || v[0] == 'T');
}

struct UringState {
    int ring_fd = -1;
    int wake_fd = -1;
    void* sq_ptr = MAP_FAILED;
    void* cq_ptr = MAP_FAILED;
    io_uring_sqe* sqes = (io_uring_sqe*) MAP_FAILED;
    size_t sq_sz = 0;
    size_t cq_sz = 0;
    size_t sqe_sz = 0;
    bool single_mmap = false;

    uint32_t* sq_head = nullptr;
    uint32_t* sq_tail = nullptr;
    uint32_t* sq_mask = nullptr;
    uint32_t* sq_entries = nullptr;
    uint32_t* sq_array = nullptr;

    uint32_t* cq_head = nullptr;
    uint32_t* cq_tail = nullptr;
    uint32_t* cq_mask = nullptr;
    io_uring_cqe* cqes = nullptr;

    uint32_t inflight = 0;

    bool init(uint32_t entries) {
        io_uring_params p;
        std::memset(&p, 0, sizeof p);
        ring_fd = (int) syscall(__NR_io_uring_setup, entries, &p);
        if (ring_fd < 0) return false;
        wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wake_fd < 0) {
            ::close(ring_fd);
            ring_fd = -1;
            return false;
        }
        sq_sz = p.sq_off.array + p.sq_entries * sizeof(uint32_t);
        cq_sz = p.cq_off.cqes + p.cq_entries * sizeof(io_uring_cqe);
        single_mmap = (p.features & IORING_FEAT_SINGLE_MMAP) != 0;
        if (single_mmap) {
            if (cq_sz > sq_sz) sq_sz = cq_sz;
            cq_sz = sq_sz;
        }
        sq_ptr = mmap(nullptr, sq_sz, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_SQ_RING);
        if (sq_ptr == MAP_FAILED) {
            destroy();
            return false;
        }
        if (single_mmap) {
            cq_ptr = sq_ptr;
        } else {
            cq_ptr = mmap(nullptr, cq_sz, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_CQ_RING);
            if (cq_ptr == MAP_FAILED) {
                destroy();
                return false;
            }
        }
        sqe_sz = p.sq_entries * sizeof(io_uring_sqe);
        sqes = (io_uring_sqe*) mmap(nullptr, sqe_sz, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_SQES);
        if (sqes == MAP_FAILED) {
            destroy();
            return false;
        }
        uint8_t* sqb = (uint8_t*) sq_ptr;
        sq_head = (uint32_t*) (sqb + p.sq_off.head);
        sq_tail = (uint32_t*) (sqb + p.sq_off.tail);
        sq_mask = (uint32_t*) (sqb + p.sq_off.ring_mask);
        sq_entries = (uint32_t*) (sqb + p.sq_off.ring_entries);
        sq_array = (uint32_t*) (sqb + p.sq_off.array);

        uint8_t* cqb = (uint8_t*) cq_ptr;
        cq_head = (uint32_t*) (cqb + p.cq_off.head);
        cq_tail = (uint32_t*) (cqb + p.cq_off.tail);
        cq_mask = (uint32_t*) (cqb + p.cq_off.ring_mask);
        cqes = (io_uring_cqe*) (cqb + p.cq_off.cqes);
        inflight = 0;
        return true;
    }

    void destroy() {
        if (sqes != MAP_FAILED) { munmap(sqes, sqe_sz); sqes = (io_uring_sqe*) MAP_FAILED; }
        if (cq_ptr != MAP_FAILED && !single_mmap) { munmap(cq_ptr, cq_sz); }
        if (sq_ptr != MAP_FAILED) { munmap(sq_ptr, sq_sz); }
        sq_ptr = cq_ptr = MAP_FAILED;
        if (wake_fd >= 0) { ::close(wake_fd); wake_fd = -1; }
        if (ring_fd >= 0) { ::close(ring_fd); ring_fd = -1; }
        inflight = 0;
    }

    bool active() const { return ring_fd >= 0; }

    void flush_queue(int fd, Ring<Pending>& queue, Ring<Completion>& done) {
        if (queue.empty()) return;
        uint32_t tail = *sq_tail;
        const uint32_t head = __atomic_load_n(sq_head, __ATOMIC_ACQUIRE);
        const uint32_t cap = *sq_entries;
        const uint32_t mask = *sq_mask;
        uint32_t submitted = 0;
        while (!queue.empty() && (tail - head) < cap) {
            const Pending p = queue.pop();
            const uint32_t idx = tail & mask;
            io_uring_sqe& sqe = sqes[idx];
            std::memset(&sqe, 0, sizeof sqe);
            sqe.opcode = IORING_OP_READ;
            sqe.fd = fd;
            sqe.off = p.offset;
            sqe.addr = (uintptr_t) p.buffer;
            sqe.len = p.length;
            sqe.user_data = p.tag;
            sq_array[idx] = idx;
            ++tail;
            ++submitted;
        }
        if (submitted == 0) return;
        __atomic_store_n(sq_tail, tail, __ATOMIC_RELEASE);
        inflight += submitted;
        for (;;) {
            const int ret = (int) syscall(__NR_io_uring_enter, ring_fd, submitted, 0, 0, nullptr, 0);
            if (ret >= 0) break;
            if (errno == EINTR) continue;
            // If kernel submission fails unexpectedly, fail the newly queued entries so callers never hang.
            break;
        }
        reap(done);
    }

    int reap(Ring<Completion>& done) {
        uint32_t head = *cq_head;
        const uint32_t tail = __atomic_load_n(cq_tail, __ATOMIC_ACQUIRE);
        const uint32_t mask = *cq_mask;
        int n = 0;
        while (head != tail) {
            const io_uring_cqe& cqe = cqes[head & mask];
            const int32_t res = cqe.res;
            done.push(Completion{cqe.user_data, res < 0 ? 0u : (uint32_t) res, res >= 0});
            if (inflight > 0) --inflight;
            ++head;
            ++n;
        }
        if (n > 0) __atomic_store_n(cq_head, head, __ATOMIC_RELEASE);
        return n;
    }
};
#endif

}  // namespace

// perf-review F-2: a pool of threads, each doing blocking O_DIRECT preads, so reads run in parallel (the thread
// count is the queue depth), or an opt-in Linux io_uring ring (STRATA_DIRECT_URING=1).
struct DirectFile::Impl {
    int fd = -1;
    uint64_t size = 0;
    std::mutex mu;
    std::condition_variable cv_work, cv_done;
    Ring<Pending> queue;
    Ring<Completion> done;
    uint32_t idle_workers = 0;
    uint32_t waiting_done = 0;
    uint32_t active_reads = 0;
    bool stop = false;
    std::vector<std::thread> pool;
#if defined(STRATA_HAVE_LINUX_IO_URING)
    UringState uring;
#endif

    void worker() {
        std::unique_lock<std::mutex> lk(mu);
        for (;;) {
            while (!stop && queue.empty()) {
                ++idle_workers;
                cv_work.wait(lk);
                --idle_workers;
            }
            if (stop && queue.empty()) return;
            const Pending p = queue.pop();
            ++active_reads;
            const bool more_work = !queue.empty() && idle_workers > 0;
            lk.unlock();
            if (more_work) cv_work.notify_one();
            const ssize_t got = pread(fd, p.buffer, p.length, (off_t) p.offset);
            const Completion c{p.tag, got < 0 ? 0u : (uint32_t) got, got >= 0};
            lk.lock();
            --active_reads;
            done.push(c);
            if (waiting_done > 0) cv_done.notify_one();
        }
    }

    void stop_pool() {
        {
            std::lock_guard<std::mutex> lk(mu);
            stop = true;
        }
        cv_work.notify_all();
        for (std::thread& t : pool) t.join();
        pool.clear();
        stop = false;
        idle_workers = 0;
        waiting_done = 0;
        active_reads = 0;
    }
};

DirectFile::DirectFile() : impl_(new Impl) {}
DirectFile::~DirectFile() { close(); delete impl_; }

bool DirectFile::open(const std::string& path, std::string& err) {
    close();
#if defined(__APPLE__)   // macOS has no O_DIRECT: F_NOCACHE keeps the reads out of the unified buffer cache
    impl_->fd = ::open(path.c_str(), O_RDONLY);
    if (impl_->fd >= 0 && fcntl(impl_->fd, F_NOCACHE, 1) != 0) { ::close(impl_->fd); impl_->fd = -1; }
#else
    impl_->fd = ::open(path.c_str(), O_RDONLY | O_DIRECT);
#endif
    if (impl_->fd < 0) { err = "DirectFile: cannot open " + path; return false; }
    struct stat st;
    if (fstat(impl_->fd, &st) != 0) { err = "DirectFile: cannot size " + path; close(); return false; }
    impl_->size = (uint64_t) st.st_size;
    impl_->queue.init(256);
    impl_->done.init(256);
#if defined(STRATA_HAVE_LINUX_IO_URING)
    if (want_direct_uring() && impl_->uring.init(128)) {
        return true;
    }
#endif
    const int n = io_threads(16);
    impl_->pool.reserve((size_t) n);
    for (int i = 0; i < n; ++i) impl_->pool.emplace_back([this] { strata::aux_cpus::pin_current_thread(); impl_->worker(); });
    return true;
}

void DirectFile::close() {
#if defined(STRATA_HAVE_LINUX_IO_URING)
    if (impl_->uring.active()) {
        std::unique_lock<std::mutex> lk(impl_->mu);
        impl_->queue.clear();
        while (impl_->uring.inflight > 0) {
            if (impl_->uring.reap(impl_->done) == 0) {
                pollfd pfd{impl_->uring.ring_fd, POLLIN, 0};
                lk.unlock();
                (void) poll(&pfd, 1, 10);
                lk.lock();
            }
        }
        impl_->uring.destroy();
    }
#endif
    impl_->stop_pool();
    if (impl_->fd >= 0) ::close(impl_->fd);
    impl_->fd = -1;
    impl_->size = 0;
    impl_->done.clear();
    impl_->queue.clear();
}

bool DirectFile::is_open() const { return impl_->fd >= 0; }
uint64_t DirectFile::size() const { return impl_->size; }

bool DirectFile::submit(uint64_t offset, void* buffer, uint32_t length, uint64_t tag, std::string& err) {
    const ReadRequest req{offset, buffer, length, tag};
    return submit_batch(&req, 1, err);
}

bool DirectFile::submit_batch(const ReadRequest* reqs, size_t n, std::string& err) {
    if (!is_open()) { err = "DirectFile: not open"; return false; }
    for (size_t i = 0; i < n; ++i) {
        if (reqs[i].offset % alignment() || reqs[i].length % alignment() ||
            ((uintptr_t) reqs[i].buffer) % alignment() || reqs[i].length == 0) {
            err = "DirectFile: unaligned request";
            return false;
        }
    }
    if (n == 0) return true;
    uint32_t to_wake = 0;
    {
        std::lock_guard<std::mutex> lk(impl_->mu);
        for (size_t i = 0; i < n; ++i) {
            impl_->queue.push(Pending{reqs[i].offset, reqs[i].buffer, reqs[i].length, reqs[i].tag});
        }
#if defined(STRATA_HAVE_LINUX_IO_URING)
        if (impl_->uring.active()) {
            impl_->uring.flush_queue(impl_->fd, impl_->queue, impl_->done);
            if (impl_->waiting_done > 0 && !impl_->done.empty()) {
                const uint64_t one = 1;
                (void) ::write(impl_->uring.wake_fd, &one, sizeof one);
            }
            return true;
        }
#endif
        to_wake = std::min<uint32_t>((uint32_t) n, impl_->idle_workers);
    }
    if (to_wake == 1) impl_->cv_work.notify_one();
    else if (to_wake > 1) impl_->cv_work.notify_all();
    return true;
}

void DirectFile::wake() {
    std::lock_guard<std::mutex> lk(impl_->mu);
    impl_->done.push(Completion{WAKE_TAG, 0, true});
#if defined(STRATA_HAVE_LINUX_IO_URING)
    if (impl_->uring.active()) {
        const uint64_t one = 1;
        (void) ::write(impl_->uring.wake_fd, &one, sizeof one);
        return;
    }
#endif
    if (impl_->waiting_done > 0) impl_->cv_done.notify_all();
}

int DirectFile::wait(Completion* out, int max, int timeout_ms) {
    std::unique_lock<std::mutex> lk(impl_->mu);
#if defined(STRATA_HAVE_LINUX_IO_URING)
    if (impl_->uring.active()) {
        impl_->uring.reap(impl_->done);
        impl_->uring.flush_queue(impl_->fd, impl_->queue, impl_->done);
        if (impl_->done.empty() && timeout_ms != 0) {
            const auto deadline = timeout_ms > 0
                ? std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms)
                : std::chrono::steady_clock::time_point::max();
            while (impl_->done.empty()) {
                int rem_ms = -1;
                if (timeout_ms > 0) {
                    const auto now = std::chrono::steady_clock::now();
                    if (now >= deadline) break;
                    rem_ms = (int) std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
                    if (rem_ms <= 0) rem_ms = 1;
                }
                pollfd pfds[2] = {
                    {impl_->uring.ring_fd, POLLIN, 0},
                    {impl_->uring.wake_fd, POLLIN, 0},
                };
                ++impl_->waiting_done;
                lk.unlock();
                const int pr = poll(pfds, 2, rem_ms);
                lk.lock();
                --impl_->waiting_done;
                if (pfds[1].revents & POLLIN) {
                    uint64_t drain_val = 0;
                    while (::read(impl_->uring.wake_fd, &drain_val, sizeof drain_val) > 0) {}
                }
                impl_->uring.reap(impl_->done);
                impl_->uring.flush_queue(impl_->fd, impl_->queue, impl_->done);
                if (pr == 0) break;
                if (pr < 0 && errno != EINTR) break;
            }
        }
        int n = 0;
        while (n < max && !impl_->done.empty()) out[n++] = impl_->done.pop();
        return n;
    }
#endif
    if (impl_->done.empty() && timeout_ms != 0) {
        ++impl_->waiting_done;
        auto ready = [&] { return !impl_->done.empty(); };
        if (timeout_ms < 0) impl_->cv_done.wait(lk, ready);
        else impl_->cv_done.wait_for(lk, std::chrono::milliseconds(timeout_ms), ready);
        --impl_->waiting_done;
    }
    int n = 0;
    while (n < max && !impl_->done.empty()) {
        out[n++] = impl_->done.pop();
    }
    return n;
}
#endif

}  // namespace strata::platform
