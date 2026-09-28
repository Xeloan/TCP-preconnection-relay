#!/usr/bin/env python3
"""Linux-only integration: real TCP, splice, epoll and /proc CPU accounting.

Run on an idle test machine: python3 tests/test_linux.py [path/to/tcp_pool.c]
Uses loopback ephemeral ports. Does not restart or alter an installed service.
"""
import concurrent.futures
import os
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
import time

if sys.platform != "linux":
    print("SKIP: real epoll/splice tests require Linux")
    sys.exit(0)

SOURCE = (Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1] / "tcp_pool.c").resolve()
WRAPPER = r'''
#define _GNU_SOURCE
#include <fcntl.h>
#include <stdarg.h>
#include <errno.h>
/* Fault injection: emulate pipe growth refused by resource limits. */
static int small_pipe_fcntl(int fd, int cmd, ...) {
    if (cmd == F_GETFL) return fcntl(fd, cmd);
    if (cmd == F_SETPIPE_SZ) return fcntl(fd, cmd, 4096);
    if (cmd == F_SETFL) {
        va_list ap; va_start(ap, cmd); int value = va_arg(ap, int); va_end(ap);
        return fcntl(fd, cmd, value);
    }
    errno = EINVAL;
    return -1;
}
#define fcntl small_pipe_fcntl
#include RELAY_SOURCE
'''


def listener():
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    sock.settimeout(15)
    return sock


def read_all(sock):
    parts = []
    while True:
        chunk = sock.recv(65536)
        if not chunk:
            return b"".join(parts)
        parts.append(chunk)


def cpu_seconds(pid):
    # comm may contain spaces, so parse after the final ')'.
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def assert_idle(process, label):
    time.sleep(0.25)
    before = cpu_seconds(process.pid)
    time.sleep(0.75)
    consumed = cpu_seconds(process.pid) - before
    print(f"{label}: {consumed:.3f}s CPU / 0.75s wall", flush=True)
    assert consumed < 0.15, f"{label}: relay busy-spins"
    assert process.poll() is None


def start_relay(binary, upstream):
    with listener() as reservation:
        port = reservation.getsockname()[1]
    env = os.environ.copy()
    env.update(LOCAL_IP="127.0.0.1", LOCAL_PORT=str(port), REMOTE_IP="127.0.0.1",
               REMOTE_TCP_PORT=str(upstream), REMOTE_UDP_PORT=str(upstream),
               POOL_SIZE="0", LOG_ENABLE="0", HALF_CLOSE_TIMEOUT="30",
               SPLICE_CHUNK=str(256 * 1024))
    process = subprocess.Popen([str(binary)], env=env, stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE, text=True)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            client = socket.create_connection(("127.0.0.1", port), timeout=0.2)
            client.settimeout(15)
            return process, client
        except ConnectionRefusedError:
            if process.poll() is not None:
                raise RuntimeError(process.stderr.read())
            time.sleep(0.02)
    process.terminate()
    process.communicate(timeout=5)
    raise TimeoutError("relay did not listen")


def stop(process):
    process.terminate()
    try:
        _, stderr = process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        _, stderr = process.communicate(timeout=5)
    if stderr:
        print(stderr, file=sys.stderr)


def connected_pair(binary, upstream):
    process, client = start_relay(binary, upstream.getsockname()[1])
    try:
        remote, _ = upstream.accept()
        remote.settimeout(15)
        return process, client, remote
    except BaseException:
        client.close()
        stop(process)
        raise


def backpressure(binary, reverse):
    payload = bytes(range(256)) * (32 * 1024)  # 8 MiB, well beyond a pipe/budget.
    with listener() as upstream:
        process, client, remote = connected_pair(binary, upstream)
        sender, receiver = (remote, client) if reverse else (client, remote)
        receiver.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32768)
        release = threading.Event()
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                def send():
                    sender.sendall(payload)
                    sender.shutdown(socket.SHUT_WR)

                def receive():
                    if not release.wait(10):
                        raise TimeoutError("receiver was not released")
                    return read_all(receiver)

                writing = pool.submit(send)
                reading = pool.submit(receive)
                try:
                    time.sleep(0.5)
                    assert not writing.done(), "test did not establish backpressure"
                    assert_idle(process, "remote->local blocked" if reverse else "local->remote blocked")
                finally:
                    release.set()
                writing.result(timeout=15)
                assert reading.result(timeout=15) == payload, "payload lost/corrupted"
                # Confirm the opposite direction survives the first FIN.
                receiver.sendall(b"reply-after-fin")
                receiver.shutdown(socket.SHUT_WR)
                assert read_all(sender) == b"reply-after-fin"
            print("PASS backpressure", "reverse" if reverse else "forward", flush=True)
        finally:
            release.set()
            client.close()
            remote.close()
            stop(process)


def half_close(binary):
    with listener() as upstream:
        process, client, remote = connected_pair(binary, upstream)
        try:
            client.sendall(b"request")
            client.shutdown(socket.SHUT_WR)
            assert read_all(remote) == b"request"
            assert_idle(process, "half-close idle")
            remote.sendall(b"delayed response")
            remote.shutdown(socket.SHUT_WR)
            assert read_all(client) == b"delayed response"
            print("PASS half_close", flush=True)
        finally:
            client.close()
            remote.close()
            stop(process)


with tempfile.TemporaryDirectory(prefix="tcp-pool-linux-") as tmp:
    wrapper = Path(tmp) / "small_pipe.c"
    binary = Path(tmp) / "tcp_pool"
    wrapper.write_text(WRAPPER)
    subprocess.run(shlex.split(os.environ.get("CC", "cc")) + [
        "-O2", "-pthread", "-Wall", "-Wextra", "-Wshadow", "-Wformat=2",
        f'-DRELAY_SOURCE="{SOURCE}"', str(wrapper), "-o", str(binary),
    ], check=True)
    backpressure(binary, reverse=False)
    backpressure(binary, reverse=True)
    half_close(binary)
