#!/usr/bin/env python3
"""Portable fault injection for the actual C forwarding functions.

Only splice/epoll/close are simulated; this is not a Linux kernel integration
test. Usage: python3 tests/test_forwarding.py [path/to/tcp_pool.c]
"""
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile

source_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1] / "tcp_pool.c"
source = source_path.read_text()
section = source[source.index("typedef struct Conn {"):source.index("//UDP转发")]

PREAMBLE = r"""
#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#undef assert
#define assert(expr) do { if (!(expr)) { fprintf(stderr, "check failed: %s (line %d)\n", #expr, __LINE__); exit(1); } } while (0)
#define EPOLLIN 1U
#define EPOLLOUT 4U
#define EPOLLRDHUP 8192U
#define EPOLLET (1U << 31)
#define EPOLL_CTL_ADD 1
#define EPOLL_CTL_DEL 2
#define EPOLL_CTL_MOD 3
#define SPLICE_F_MOVE 1
#define SPLICE_F_NONBLOCK 2
#define TAG_CONN_SIDE ((uintptr_t)1)
static size_t cfg_splice_chunk = 256 * 1024;
struct epoll_event { uint32_t events; union { void *ptr; } data; };
static size_t available, in_pipe, delivered, capacity, write_limit;
static bool source_eof, read_intr, write_intr, zero_write;
static int calls, mods, adds, epoll_fail;
static uint32_t masks[32];
static int epoll_ctl(int ep, int op, int fd, struct epoll_event *ev) {
    (void)ep;
    if (op == EPOLL_CTL_DEL) { masks[fd] = 0; return 0; }
    if (epoll_fail) { errno = ENOMEM; return -1; }
    if (op == EPOLL_CTL_MOD) mods++;
    if (op == EPOLL_CTL_ADD) adds++;
    masks[fd] = ev->events;
    return 0;
}
static void safe_close(int *fd) { *fd = -1; }
static ssize_t splice(int in, void *off_in, int out, void *off_out, size_t n, unsigned flags) {
    (void)off_in; (void)off_out; (void)flags;
    assert(++calls < 10000); /* Bound every test even if a loop regresses. */
    if (in == 10 && out == 12) {
        if (read_intr) { read_intr = false; errno = EINTR; return -1; }
        if (in_pipe == capacity) { errno = EAGAIN; return -1; }
        if (!available) { if (source_eof) return 0; errno = EAGAIN; return -1; }
        if (n > available) n = available;
        if (n > capacity - in_pipe) n = capacity - in_pipe;
        available -= n; in_pipe += n;
        return (ssize_t)n;
    }
    assert(in == 13 && out == 11);
    if (write_intr) { write_intr = false; errno = EINTR; return -1; }
    if (zero_write) return 0;
    if (!write_limit) { errno = EAGAIN; return -1; }
    if (n > write_limit) n = write_limit;
    if (n > in_pipe) n = in_pipe;
    write_limit -= n; in_pipe -= n; delivered += n;
    return (ssize_t)n;
}
"""

TESTS = r"""
/* Allow baseline comparison even before PUMP_MORE existed. */
#define PUMP_MORE 3
static Conn fresh(void) {
    available = in_pipe = delivered = 0;
    capacity = 4096; write_limit = SIZE_MAX;
    source_eof = read_intr = write_intr = zero_write = false;
    calls = mods = adds = epoll_fail = 0;
    memset(masks, 0, sizeof(masks));
    Conn c = {0};
    c.fd_l = 10; c.fd_r = 11;
    c.pipe_l2r[0] = 13; c.pipe_l2r[1] = 12;
    c.pipe_r2l[0] = c.pipe_r2l[1] = -1;
    return c;
}
static pump_status_t transfer(Conn *c) {
    return pump(10, 11, 12, 13, &c->len_l2r, 123, &c->last_l2r);
}
static void small_pipe(void) {
    Conn c = fresh();
    available = 50000; source_eof = true;
    assert(transfer(&c) == PUMP_EOF);
    assert(delivered == 50000 && !available && !in_pipe && !c.len_l2r);
    assert(c.last_l2r == 123);
}
static void backpressure(void) {
    Conn c = fresh();
    available = 50000; write_limit = 0;
    assert(transfer(&c) == PUMP_OK);
    assert(c.len_l2r == capacity && delivered == 0);
    conn_watch(&c);
    assert(!(masks[10] & (EPOLLIN | EPOLLRDHUP)));
    assert(masks[11] & EPOLLOUT);
    int old_mods = mods;
    for (int i = 0; i < 100; i++) {
        assert(transfer(&c) == PUMP_OK);
        conn_watch(&c);
    }
    assert(mods == old_mods && delivered == 0);
    write_limit = SIZE_MAX; source_eof = true;
    assert(transfer(&c) == PUMP_EOF);
    assert(delivered == 50000 && !c.len_l2r);
}
static void stable_watch(void) {
    Conn c = fresh();
    c.eof_l2r = true; /* A socket may keep reporting HUP after EOF. */
    conn_watch(&c);
    int old_mods = mods;
    for (int i = 0; i < 100; i++) conn_watch(&c);
    assert(mods == old_mods);
    assert(!(masks[10] & (EPOLLIN | EPOLLRDHUP)));
    assert(masks[11] & EPOLLIN);
}
static void budget(void) {
    Conn c = fresh();
    available = 3 * cfg_splice_chunk + 17; source_eof = true;
    for (int i = 1; i <= 3; i++) {
        assert(transfer(&c) == PUMP_MORE);
        assert(delivered == (size_t)i * cfg_splice_chunk);
        assert(!in_pipe && !c.len_l2r);
    }
    assert(transfer(&c) == PUMP_EOF);
    assert(delivered == 3 * cfg_splice_chunk + 17);
}
static void interrupted(void) {
    Conn c = fresh();
    available = 100; source_eof = read_intr = write_intr = true;
    assert(transfer(&c) == PUMP_EOF);
    assert(delivered == 100);
}
static void partial_write(void) {
    Conn c = fresh();
    available = 20000; write_limit = 123; source_eof = true;
    assert(transfer(&c) == PUMP_OK);
    assert(delivered == 123 && c.len_l2r == capacity - 123);
    write_limit = SIZE_MAX;
    assert(transfer(&c) == PUMP_EOF);
    assert(delivered == 20000 && !c.len_l2r);
}
static void zero_drain(void) {
    Conn c = fresh();
    in_pipe = c.len_l2r = 10; zero_write = true; errno = EAGAIN;
    assert(transfer(&c) == PUMP_ERR); /* errno is meaningless on success. */
}
static void idle(void) {
    Conn c = fresh();
    assert(transfer(&c) == PUMP_OK);
    assert(calls == 1 && !c.last_l2r);
}
static void connecting(void) {
    Conn c = fresh(); c.connecting = true;
    conn_watch(&c);
    assert(!(masks[10] & EPOLLIN) && (masks[11] & EPOLLOUT));
    c.connecting = false;
    conn_watch(&c);
    assert((masks[10] & EPOLLIN) && (masks[11] & EPOLLIN));
    assert(!(masks[11] & EPOLLOUT));
}
static void watch_failure(void) {
    Conn c = fresh(); epoll_fail = 1;
    conn_watch(&c);
    assert(c.closed && c.fd_l == -1 && c.fd_r == -1);
}
int main(int argc, char **argv) {
    (void)conn_list;
    assert(argc == 2);
    struct { const char *name; void (*fn)(void); } cases[] = {
        {"small_pipe", small_pipe}, {"backpressure", backpressure},
        {"stable_watch", stable_watch}, {"budget", budget},
        {"interrupted", interrupted}, {"partial_write", partial_write},
        {"zero_drain", zero_drain}, {"idle", idle},
        {"connecting", connecting}, {"watch_failure", watch_failure},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        if (!strcmp(argv[1], cases[i].name)) { cases[i].fn(); return 0; }
    }
    return 2;
}
"""

with tempfile.TemporaryDirectory(prefix="tcp-pool-test-") as tmp:
    src = Path(tmp) / "forwarding.c"
    binary = Path(tmp) / "forwarding"
    src.write_text(PREAMBLE + section + TESTS)
    subprocess.run(shlex.split(os.environ.get("CC", "cc")) + [
        "-std=c11", "-Wall", "-Wextra", "-Werror", "-O1",
        "-fsanitize=address,undefined", str(src), "-o", str(binary),
    ], check=True)
    failures = 0
    for case in ["small_pipe", "backpressure", "stable_watch", "budget", "interrupted",
                 "partial_write", "zero_drain", "idle", "connecting", "watch_failure"]:
        result = subprocess.run([str(binary), case], capture_output=True, text=True, timeout=5)
        print(f"{'PASS' if result.returncode == 0 else 'FAIL'} {case}")
        if result.returncode:
            failures += 1
            print(result.stderr.strip())
    sys.exit(bool(failures))
