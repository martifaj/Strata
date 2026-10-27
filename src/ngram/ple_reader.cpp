// src/ngram/ple_reader.cpp - see include/strata/ngram/ple_reader.hpp.
#include "strata/ngram/ple_reader.hpp"
#include "strata/platform/aux_cpus.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <exception>
#include <mutex>
#include <thread>

namespace strata::ngram {

using platform::Completion;
using platform::DirectFile;
using platform::now_us;

double ReaderStats::percentile(double q) const {
    if (read_us.empty()) return 0.0;
    std::vector<float> v(read_us);
    const size_t k = std::min(v.size() - 1, (size_t) (q * (double) (v.size() - 1) + 0.5));
    std::nth_element(v.begin(), v.begin() + (ptrdiff_t) k, v.end());
    return v[k];
}

namespace {

constexpr size_t LATENCY_RING = 65536;
constexpr uint32_t WAYS = 8;
constexpr uint32_t EMPTY = 0xFFFFFFFFu;

/// Set-associative row cache: 8 ways per set, round-robin replacement inside a set. Bounded by construction.
struct RowCache {
    uint64_t sets = 0;
    uint32_t rb = ROW_BYTES;        // bytes per row
    std::vector<uint32_t> keys;     // sets * WAYS
    std::vector<uint8_t> data;      // sets * WAYS * rb
    std::vector<uint8_t> next;      // per-set replacement pointer
    uint64_t used = 0;

    void init(uint64_t rows, uint32_t row_bytes = ROW_BYTES) {
        sets = rows / WAYS;
        rb = row_bytes;
        keys.assign(sets * WAYS, EMPTY);
        data.assign(sets * WAYS * rb, 0);
        next.assign(sets, 0);
        used = 0;
    }
    static uint64_t mix(uint32_t r) {
        uint64_t x = r * 0x9E3779B97F4A7C15ull;
        return x ^ (x >> 29);
    }
    const uint8_t* find(uint32_t row) const {
        if (sets == 0) return nullptr;
        const uint64_t s = mix(row) % sets;
        for (uint32_t w = 0; w < WAYS; ++w)
            if (keys[s * WAYS + w] == row) return &data[(s * WAYS + w) * rb];
        return nullptr;
    }
    void insert(uint32_t row, const uint8_t* bytes) {
        if (sets == 0 || find(row) != nullptr) return;
        const uint64_t s = mix(row) % sets;
        uint32_t w = next[s];
        next[s] = (uint8_t) ((w + 1) % WAYS);
        if (keys[s * WAYS + w] == EMPTY) ++used;
        keys[s * WAYS + w] = row;
        std::memcpy(&data[(s * WAYS + w) * rb], bytes, rb);
    }
};

struct Use {
    uint32_t row;
    uint32_t in_page;      // byte offset of the row inside the read buffer
    uint8_t* dst;
};

struct Job {
    uint64_t offset = 0;   // aligned file offset
    uint32_t length = 0;   // PAGE or 2 * PAGE (a row that straddles a page boundary)
    uint32_t ticket = 0;
    std::vector<Use> uses;
    double issued_us = 0;
    bool keepalive = false;// no rows and no ticket: it only keeps the SSD awake (`set_keepalive`)
};

struct Miss {
    uint64_t first;
    uint32_t length;
    uint32_t row;
    uint32_t in_page;
    uint32_t order;
    uint8_t* dst;
};

/// Pre-allocated circular queue of `Job`s that recycles each slot's `std::vector<Use>` capacity across jobs.
struct JobQueue {
    std::vector<Job> buf;
    size_t head = 0;
    size_t count = 0;

    void init(size_t cap = 256) {
        buf.resize(cap);
        for (Job& j : buf) {
            j.uses.clear();
            j.keepalive = false;
        }
        head = 0;
        count = 0;
    }
    bool empty() const { return count == 0; }
    size_t size() const { return count; }
    Job& front() { return buf[head]; }
    void pop_front() {
        head = (head + 1) & (buf.size() - 1);
        --count;
    }
    Job& push_slot() {
        if (count == buf.size()) grow();
        Job& slot = buf[(head + count) & (buf.size() - 1)];
        ++count;
        slot.uses.clear();
        slot.keepalive = false;
        return slot;
    }
    void clear() {
        for (size_t i = 0; i < count; ++i) {
            buf[(head + i) & (buf.size() - 1)].uses.clear();
        }
        head = 0;
        count = 0;
    }
    void grow() {
        const size_t old_cap = buf.size();
        const size_t new_cap = old_cap ? old_cap * 2 : 256;
        std::vector<Job> next(new_cap);
        for (size_t i = 0; i < count; ++i) {
            std::swap(next[i], buf[(head + i) & (old_cap - 1)]);
        }
        buf.swap(next);
        head = 0;
    }
};

/// Flat power-of-2 hash table for active tickets with backshift deletion (zero heap allocations in steady state).
struct TicketTable {
    struct Entry {
        uint32_t id = 0;       // 0 = empty
        uint32_t pending = 0;
    };
    std::vector<Entry> tab;
    size_t count = 0;

    void init(size_t cap = 256) {
        tab.assign(cap, Entry{});
        count = 0;
    }
    void clear() {
        std::fill(tab.begin(), tab.end(), Entry{});
        count = 0;
    }
    static size_t hash(uint32_t id) {
        uint64_t x = id * 0x9E3779B97F4A7C15ull;
        return (size_t) (x ^ (x >> 32));
    }
    void insert(uint32_t id, uint32_t pending) {
        if (tab.empty()) init(256);
        if ((count + 1) * 2 >= tab.size()) grow();
        const size_t mask = tab.size() - 1;
        size_t idx = hash(id) & mask;
        while (tab[idx].id != 0 && tab[idx].id != id) idx = (idx + 1) & mask;
        if (tab[idx].id == 0) ++count;
        tab[idx].id = id;
        tab[idx].pending = pending;
    }
    uint32_t* find_pending(uint32_t id) {
        if (id == 0 || tab.empty()) return nullptr;
        const size_t mask = tab.size() - 1;
        size_t idx = hash(id) & mask;
        while (tab[idx].id != 0) {
            if (tab[idx].id == id) return &tab[idx].pending;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
    /// Decrements `pending` if > 0; returns true iff `pending` reached 0.
    bool decrement(uint32_t id) {
        uint32_t* p = find_pending(id);
        if (p == nullptr) return false;
        if (*p > 0) --(*p);
        return *p == 0;
    }
    void erase(uint32_t id) {
        if (id == 0 || tab.empty()) return;
        const size_t mask = tab.size() - 1;
        size_t idx = hash(id) & mask;
        while (tab[idx].id != id) {
            if (tab[idx].id == 0) return;
            idx = (idx + 1) & mask;
        }
        size_t next = (idx + 1) & mask;
        while (tab[next].id != 0) {
            const size_t home = hash(tab[next].id) & mask;
            if (((next - home) & mask) >= ((next - idx) & mask)) {
                tab[idx] = tab[next];
                idx = next;
            }
            next = (next + 1) & mask;
        }
        tab[idx] = Entry{};
        --count;
    }
    void grow() {
        std::vector<Entry> old = std::move(tab);
        tab.assign(old.empty() ? 256 : old.size() * 2, Entry{});
        count = 0;
        const size_t mask = tab.size() - 1;
        for (const Entry& e : old) {
            if (e.id == 0) continue;
            size_t idx = hash(e.id) & mask;
            while (tab[idx].id != 0) idx = (idx + 1) & mask;
            tab[idx] = e;
            ++count;
        }
    }
};

}  // namespace

// THREADING. With `io_thread` (the default) one worker thread owns every DirectFile call: it submits queued jobs
// and reaps completions, so `issue` on the caller's thread only builds jobs and wakes it (~11 us per ReadFile
// no longer lands on the host loop). `mu` guards everything below except `file`, which only the worker touches
// (plus `wake`, which is thread-safe). Without `io_thread` the caller does all of it, as before.
struct PleReader::Impl {
    DirectFile file;
    uint64_t table_offset = 0;
    uint64_t n_rows = 0;
    uint32_t row_bytes = ROW_BYTES;
    uint32_t max_inflight = 0;
    uint8_t* slab = nullptr;              // max_inflight slots of 2 pages
    std::vector<uint32_t> free_slots;
    std::vector<Job> inflight;            // indexed by slot
    std::vector<Completion> delayed;      // completed but held back by fault injection
    JobQueue queue;                       // not yet submitted
    TicketTable tickets;
    std::vector<Miss> miss_scratch;       // reused across large (>64 row) issue() calls
    uint32_t next_ticket = 1;
    RowCache cache;
    ReaderStats stats;
    size_t ring_pos = 0;
    double delay_us = 0;
    std::string error;
    // the keep-alive (`set_keepalive`), steady-clock microseconds; `keep_us` 0 = off
    double keep_us = 0;
    double keep_window_us = 0;
    double last_issue_us = 0;             // the last `issue`
    double last_read_us = 0;              // the last read that went out, rows or keep-alive
    uint64_t rng = 0x9E3779B97F4A7C15ull;

    bool threaded = false;
    bool stop = false;
    bool in_file_wait = false;
    bool wake_sent = false;
    bool ticket_done = false;
    uint32_t waiting_collectors = 0;
    std::mutex mu;
    std::condition_variable cv_work;      // worker: there is something to submit
    std::condition_variable cv_done;      // collectors: a ticket may have completed
    std::thread worker;

    uint8_t* slot_buf(uint32_t s) { return slab + (size_t) s * 2 * PAGE; }
    bool busy() const { return free_slots.size() < max_inflight || !delayed.empty(); }

    void dec_ticket(uint32_t ticket) {
        if (tickets.decrement(ticket)) ticket_done = true;
    }

    void notify_collectors() {
        if (waiting_collectors == 1) cv_done.notify_one();
        else if (waiting_collectors > 1) cv_done.notify_all();
    }

    void cancel_queued() {
        while (!queue.empty()) {
            Job& job = queue.front();
            dec_ticket(job.ticket);
            job.uses.clear();
            queue.pop_front();
        }
        ticket_done = true;
    }

    void record_latency(double us) {
        stats.read_us_sum += us;
        if (stats.read_us.size() < LATENCY_RING) stats.read_us.push_back((float) us);
        else stats.read_us[ring_pos++ % LATENCY_RING] = (float) us;
    }

    bool pump() {
        platform::ReadRequest batch[64];
        uint32_t batch_slots[64];
        size_t nb = 0;
        auto flush_batch = [&]() -> bool {
            if (nb == 0) return true;
            const double t0 = now_us();
            for (size_t i = 0; i < nb; ++i) inflight[batch_slots[i]].issued_us = t0;
            if (!file.submit_batch(batch, nb, error)) {
                for (size_t i = 0; i < nb; ++i) {
                    const uint32_t s = batch_slots[i];
                    Job& j = inflight[s];
                    j.uses.clear();
                    dec_ticket(j.ticket);
                    free_slots.push_back(s);
                }
                nb = 0;
                cancel_queued();
                return false;
            }
            const double t1 = now_us();
            last_read_us = t0;
            stats.submit_us += t1 - t0;
            stats.reads += nb;
            nb = 0;
            return true;
        };
        while (!queue.empty() && !free_slots.empty()) {
            if (queue.front().keepalive) {
                if (!flush_batch()) return false;
                const uint32_t s = free_slots.back();
                free_slots.pop_back();
                std::swap(inflight[s], queue.front());
                queue.pop_front();
                Job& j = inflight[s];
                j.issued_us = now_us();
                std::string kerr;             // a keep-alive read that cannot go out must not fail the reader
                if (!file.submit(j.offset, slot_buf(s), j.length, s, kerr)) {
                    keep_us = 0;
                    j.keepalive = false;
                    free_slots.push_back(s);
                    continue;
                }
                last_read_us = j.issued_us;
                continue;
            }
            const uint32_t s = free_slots.back();
            free_slots.pop_back();
            std::swap(inflight[s], queue.front());
            queue.pop_front();
            Job& j = inflight[s];
            batch[nb] = platform::ReadRequest{j.offset, slot_buf(s), j.length, s};
            batch_slots[nb] = s;
            ++nb;
            if (nb == 64 && !flush_batch()) return false;
        }
        return flush_batch();
    }

    /// When the next keep-alive read is due, or < 0 when none is: it is off, the reader failed, or no rows
    /// were asked for within the window (then the SSD may sleep; the next `issue` re-arms it).
    double keepalive_due(double now) const {
        if (keep_us <= 0 || !error.empty() || last_issue_us <= 0 || now - last_issue_us > keep_window_us) return -1;
        return last_read_us + keep_us;
    }

    /// One page of the table, a different one each time, so the SSD really reads (not its controller's buffer).
    void queue_keepalive() {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        const uint64_t first = table_offset / PAGE, end = (table_offset + n_rows * (uint64_t) row_bytes) / PAGE;
        Job& j = queue.push_slot();
        j.offset = (first + (end > first ? rng % (end - first) : 0)) * PAGE;
        j.length = PAGE;
        j.keepalive = true;
    }

    bool finish(const Completion& c) {
        const uint32_t s = (uint32_t) c.tag;
        if (s >= inflight.size()) {
            error = "PleReader: invalid table-read completion";
            cancel_queued();
            return false;
        }
        Job& j = inflight[s];
        if (j.keepalive) {                         // no rows: it counts once it is done, with how long it took;
            if (c.ok) {                            // a failed one turns the keep-alive off, not the reader
                ++stats.keepalive_reads;
                stats.keepalive_us_max = std::max(stats.keepalive_us_max, now_us() - j.issued_us);
            } else {
                keep_us = 0;
            }
            j.keepalive = false;
            free_slots.push_back(s);
            return error.empty() ? pump() : true;
        }
        if (!c.ok) {
            error = "PleReader: a table read failed";
            cancel_queued();
            j.uses.clear();
            dec_ticket(j.ticket);
            free_slots.push_back(s);
            return false;
        }
        record_latency(now_us() - j.issued_us);
        stats.bytes += c.bytes;
        const uint8_t* buf = slot_buf(s);
        for (const Use& u : j.uses) {
            if (u.in_page + row_bytes > c.bytes) {
                error = "PleReader: short read inside the table";
                cancel_queued();
                j.uses.clear();
                dec_ticket(j.ticket);
                free_slots.push_back(s);
                return false;
            }
        }
        for (const Use& u : j.uses) {
            std::memcpy(u.dst, buf + u.in_page, row_bytes);
            cache.insert(u.row, buf + u.in_page);
        }
        dec_ticket(j.ticket);
        j.uses.clear();
        free_slots.push_back(s);
        return error.empty() ? pump() : true;
    }

    /// Completions (or wake packets) just returned by `file.wait`, applying fault injection.
    bool process(const Completion* got, int n) {
        bool ok = true;
        for (int i = 0; i < n; ++i) {
            if (got[i].tag == DirectFile::WAKE_TAG) continue;
            if (got[i].tag >= inflight.size()) {
                error = "PleReader: invalid table-read completion";
                cancel_queued();
                ok = false;
                continue;
            }
            if (delay_us > 0 && now_us() - inflight[(uint32_t) got[i].tag].issued_us < delay_us) {
                delayed.push_back(got[i]);
                ++stats.late_injected;
                continue;
            }
            if (!finish(got[i])) ok = false;   // drain the rest of this batch so no completed slot is stranded
        }
        return ok;
    }

    bool release_delayed() {
        const double now = now_us();
        for (size_t i = 0; i < delayed.size();) {
            if (now - inflight[(uint32_t) delayed[i].tag].issued_us >= delay_us) {
                const Completion c = delayed[i];
                delayed.erase(delayed.begin() + (ptrdiff_t) i);
                if (!finish(c)) return false;
            } else {
                ++i;
            }
        }
        return true;
    }

    /// Caller-thread mode: process whatever has completed; blocks up to `timeout_ms` for the first completion.
    bool drain(int timeout_ms) {
        Completion got[64];
        if (!delayed.empty()) {
            if (!release_delayed()) return false;
            timeout_ms = 0;                        // keep polling the held completions
        }
        const int n = file.wait(got, 64, timeout_ms);
        return process(got, n);
    }

    void worker_loop() {
        std::unique_lock<std::mutex> lk(mu);
        for (;;) {
            // Idle: wait for work. While rows are being asked for, the wait ends in time for a keep-alive read.
            while (!(stop || !queue.empty() || busy())) {
                const double now = now_us();
                const double due = keepalive_due(now);
                if (due < 0) cv_work.wait(lk);
                else if (now >= due) queue_keepalive();
                else cv_work.wait_for(lk, std::chrono::microseconds(std::max<int64_t>(1000, (int64_t) (due - now))));
            }
            if (stop && !busy()) break;
            if (error.empty() && !pump() && error.empty()) error = "PleReader: submit failed";
            if (!delayed.empty() && !release_delayed() && error.empty()) error = "PleReader: read failed";
            if (!error.empty()) {
                ticket_done = false;
                notify_collectors();
                if (!busy()) { cv_work.wait(lk, [&] { return stop; }); break; }
            }
            if (!busy()) {
                if (ticket_done) {
                    ticket_done = false;
                    notify_collectors();
                }
                continue;
            }
            // Block in the port WITHOUT the lock, so `issue` can queue work; `issue` wakes us with a packet only when
            // we are actually waiting here and have free slots to pump.
            const int timeout = delayed.empty() ? -1 : 0;
            in_file_wait = true;
            wake_sent = false;
            lk.unlock();
            Completion got[64];
            const int n = file.wait(got, 64, timeout);
            lk.lock();
            in_file_wait = false;
            wake_sent = false;
            if (!process(got, n) && error.empty()) error = "PleReader: read failed";
            if (ticket_done || !error.empty()) {
                ticket_done = false;
                notify_collectors();
            }
        }
    }
};

PleReader::PleReader() : impl_(new Impl) {}
PleReader::~PleReader() {
    close();
    delete impl_;
}

bool PleReader::open(const std::string& path, uint64_t table_offset, uint64_t n_rows, uint32_t max_inflight,
                     uint64_t cache_rows, std::string& err, bool io_thread, uint32_t row_bytes) {
    close();
    if (max_inflight == 0 || max_inflight > 1024) { err = "PleReader: max_inflight must be 1..1024"; return false; }
    if (row_bytes == 0 || row_bytes > PAGE) { err = "PleReader: row_bytes must be 1..4096"; return false; }
    if (!impl_->file.open(path, err)) return false;
    impl_->row_bytes = row_bytes;
    if (table_offset + n_rows * (uint64_t) row_bytes > impl_->file.size()) {
        err = "PleReader: the table extends past the end of " + path;
        close();
        return false;
    }
    impl_->table_offset = table_offset;
    impl_->n_rows = n_rows;
    impl_->max_inflight = max_inflight;
    impl_->slab = (uint8_t*) DirectFile::alloc_aligned((size_t) max_inflight * 2 * PAGE);
    if (impl_->slab == nullptr) { err = "PleReader: cannot allocate read buffers"; close(); return false; }
    impl_->inflight.assign(max_inflight, Job{});
    impl_->free_slots.clear();
    for (uint32_t s = max_inflight; s-- > 0;) impl_->free_slots.push_back(s);
    impl_->queue.init(256);
    impl_->tickets.init(256);
    impl_->cache.init(cache_rows, row_bytes);
    impl_->error.clear();
    impl_->keep_us = 0;
    impl_->last_issue_us = impl_->last_read_us = 0;
    impl_->rng = 0x9E3779B97F4A7C15ull ^ (uint64_t) now_us();
    if (impl_->rng == 0) impl_->rng = 1;
    reset_stats();
    impl_->stop = false;
    impl_->in_file_wait = false;
    impl_->wake_sent = false;
    impl_->ticket_done = false;
    impl_->waiting_collectors = 0;
    impl_->threaded = io_thread;
    if (io_thread) {
        try {
            impl_->worker = std::thread([this] { strata::aux_cpus::pin_current_thread(); impl_->worker_loop(); });
        } catch (const std::exception& e) {
            err = std::string("PleReader: cannot create I/O worker: ") + e.what();
            close();
            return false;
        } catch (...) {
            err = "PleReader: cannot create I/O worker";
            close();
            return false;
        }
    }
    return true;
}

void PleReader::close() {
    Impl& m = *impl_;
    if (m.worker.joinable()) {
        {
            std::lock_guard<std::mutex> lk(m.mu);
            m.stop = true;
            m.queue.clear();                       // unsubmitted work is dropped; in-flight reads still drain
        }
        m.cv_work.notify_all();
        m.file.wake();
        m.worker.join();
    }
    // Outstanding reads must finish before their buffers are released (caller-thread mode, or a worker that
    // stopped on an error).
    if (m.file.is_open()) {
        while (m.free_slots.size() < m.max_inflight) {
            Completion c[64];
            const int n = m.file.wait(c, 64, -1);
            if (n == 0) break;
            for (int i = 0; i < n; ++i)
                if (c[i].tag != DirectFile::WAKE_TAG) m.free_slots.push_back((uint32_t) c[i].tag);
        }
    }
    m.file.close();
    DirectFile::free_aligned(m.slab);
    m.slab = nullptr;
    m.queue.clear();
    m.tickets.clear();
    m.delayed.clear();
    m.inflight.clear();
    m.free_slots.clear();
    m.cache.init(0);
    m.threaded = false;
    m.stop = false;
    m.in_file_wait = false;
    m.wake_sent = false;
    m.ticket_done = false;
    m.waiting_collectors = 0;
    m.keep_us = 0;
    m.last_issue_us = m.last_read_us = 0;
}

bool PleReader::is_open() const { return impl_->file.is_open(); }
uint32_t PleReader::row_bytes() const { return impl_->row_bytes; }

PleReader::Ticket PleReader::issue(const uint32_t* rows, size_t n, uint8_t* out_raw) {
    Impl& m = *impl_;
    std::unique_lock<std::mutex> lk(m.mu, std::defer_lock);
    if (m.threaded) lk.lock();
    const uint32_t id = m.next_ticket++;
    if (m.next_ticket == 0) m.next_ticket = 1;
    const double now = now_us();
    const bool rearm = m.keep_us > 0 && (m.last_issue_us <= 0 || now - m.last_issue_us > m.keep_window_us);
    m.last_issue_us = now;
    // After a failed read the reader stays failed (`collect` reports `error` until the next `open`) and its worker has
    // parked (worker_loop). A ticket queued now would never be read and `collect` would wait for it for ever, so it
    // is not queued: it has no jobs, and `collect` returns the error at once.
    if (!m.error.empty()) {
        m.tickets.insert(id, 0);
        return Ticket{id};
    }
    Miss stack_misses[64];
    Miss* misses = stack_misses;
    if (n > 64) {
        if (m.miss_scratch.size() < n) m.miss_scratch.resize(n);
        misses = m.miss_scratch.data();
    }
    size_t n_miss = 0;
    const uint32_t rb = m.row_bytes;
    for (size_t i = 0; i < n; ++i) {
        uint8_t* dst = out_raw + i * rb;
        ++m.stats.requests;
        if (rows[i] >= m.n_rows) {
            std::memset(dst, 0, rb);
            continue;
        }
        if (const uint8_t* hit = m.cache.find(rows[i])) {
            std::memcpy(dst, hit, rb);
            ++m.stats.cache_hits;
            continue;
        }
        const uint64_t at = m.table_offset + (uint64_t) rows[i] * rb;
        const uint64_t first = at / PAGE * PAGE;
        const uint32_t length = (uint32_t) ((at + rb - 1) / PAGE * PAGE - first + PAGE);
        misses[n_miss++] = Miss{first, length, rows[i], (uint32_t) (at - first), (uint32_t) i, dst};
    }
    // Sorted by offset: prefill chunks then read the SSD in near-sequential order, and equal pages sit adjacent.
    if (n_miss > 1) {
        std::sort(misses, misses + n_miss, [](const Miss& a, const Miss& b) {
            return a.first != b.first ? a.first < b.first : a.order < b.order;
        });
    }
    uint32_t n_jobs = 0;
    for (size_t k = 0; k < n_miss;) {
        const uint64_t first = misses[k].first;
        Job& j = m.queue.push_slot();
        j.offset = first;
        j.length = misses[k].length;
        j.ticket = id;
        j.uses.push_back(Use{misses[k].row, misses[k].in_page, misses[k].dst});
        ++k;
        while (k < n_miss && misses[k].first == first) {
            j.length = std::max(j.length, misses[k].length);
            j.uses.push_back(Use{misses[k].row, misses[k].in_page, misses[k].dst});
            ++m.stats.dedup_rows;
            ++k;
        }
        ++n_jobs;
    }
    m.tickets.insert(id, n_jobs);
    const bool has_jobs = n_jobs > 0;
    if (m.threaded) {
        bool wake_cv = false;
        bool wake_file = false;
        if (has_jobs) {
            if (m.in_file_wait) {
                if (!m.wake_sent && (!m.free_slots.empty() || !m.delayed.empty())) {
                    m.wake_sent = true;
                    wake_file = true;
                }
            } else {
                wake_cv = true;
            }
        } else if (rearm && !m.in_file_wait) {
            wake_cv = true;                        // the keep-alive had lapsed: start it again
        }
        lk.unlock();
        if (wake_cv) m.cv_work.notify_one();
        if (wake_file) m.file.wake();              // only when the worker is blocked in `file.wait` with free slots
    } else if (!m.pump() && m.error.empty()) {
        m.error = "PleReader: submit failed";
    }
    return Ticket{id};
}

bool PleReader::collect(Ticket t, std::string& err) {
    Impl& m = *impl_;
    const double start = now_us();
    if (m.threaded) {
        std::unique_lock<std::mutex> lk(m.mu);
        uint32_t* p = m.tickets.find_pending(t.id);
        if (p == nullptr) { err = "PleReader: unknown ticket"; return false; }
        if (*p > 0) {
            ++m.waiting_collectors;
            m.cv_done.wait(lk, [&] {
                const uint32_t* cur = m.tickets.find_pending(t.id);
                return cur == nullptr || *cur == 0;
            });
            --m.waiting_collectors;
            p = m.tickets.find_pending(t.id);
            if (p == nullptr) { err = "PleReader: unknown ticket"; return false; }
        }
        if (!m.error.empty()) { err = m.error; m.tickets.erase(t.id); return false; }
        m.stats.wait_us += now_us() - start;
        m.tickets.erase(t.id);
        return true;
    }
    uint32_t* p = m.tickets.find_pending(t.id);
    if (p == nullptr) { err = "PleReader: unknown ticket"; return false; }
    while (*p > 0) {
        const bool drained = m.drain(-1);
        if (!drained && m.error.empty()) m.error = "PleReader: read failed";
        p = m.tickets.find_pending(t.id);
        if (p == nullptr) { err = "PleReader: unknown ticket"; return false; }
    }
    if (!m.error.empty()) { err = m.error; m.tickets.erase(t.id); return false; }
    m.stats.wait_us += now_us() - start;
    m.tickets.erase(t.id);
    return true;
}

void PleReader::set_keepalive(double period_ms, double window_s) {
    Impl& m = *impl_;
    {
        std::lock_guard<std::mutex> lk(m.mu);
        m.keep_us = m.threaded && period_ms > 0 ? period_ms * 1000.0 : 0.0;
        m.keep_window_us = window_s > 0 ? window_s * 1e6 : 0.0;
    }
    m.cv_work.notify_all();
}

void PleReader::set_injected_delay_us(double delay_us) {
    std::lock_guard<std::mutex> lk(impl_->mu);
    impl_->delay_us = delay_us < 0 ? 0 : delay_us;
}
const ReaderStats& PleReader::stats() const { return impl_->stats; }
ReaderStats PleReader::snapshot() const {
    Impl& m = *impl_;
    std::unique_lock<std::mutex> lk(m.mu, std::defer_lock);
    if (m.threaded) lk.lock();
    return m.stats;
}
void PleReader::reset_stats() {
    Impl& m = *impl_;
    std::unique_lock<std::mutex> lk(m.mu, std::defer_lock);
    if (m.threaded) lk.lock();                     // the worker may be counting a keep-alive read
    m.stats = ReaderStats{};
    m.ring_pos = 0;
}
uint64_t PleReader::cache_capacity() const { return impl_->cache.sets * WAYS; }
uint64_t PleReader::cache_size() const { return impl_->cache.used; }

}  // namespace strata::ngram
