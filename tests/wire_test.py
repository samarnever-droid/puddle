#!/usr/bin/env python3
"""puddle M7 wire resilience: the server must survive arbitrary hostile
input on the socket — garbage, truncation, lies about packet length, random
kinds, abrupt disconnects — and keep serving normal clients afterwards.

The server may answer with an error or drop the connection (both fine);
it must never crash, hang, or stop accepting connections.
"""
import os
import random
import shutil
import socket
import struct
import subprocess
import sys
import time

import psycopg2

BIN = "/home/user/puddle/target/release/puddle"
DATA = "/tmp/puddle-wire"
PORT = 5495
DSN = f"host=127.0.0.1 port={PORT} dbname=x user=u sslmode=disable"
random.seed(20261023)
FAILS = []


def start():
    shutil.rmtree(DATA, ignore_errors=True)
    p = subprocess.Popen([BIN, "--data", f"file://{DATA}", "--port", str(PORT),
                          "--bind", "127.0.0.1"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(200):
        try:
            psycopg2.connect(DSN, connect_timeout=1).close()
            return p
        except Exception:
            time.sleep(0.05)
    raise RuntimeError("no start")


def alive(p):
    return p.poll() is None


def healthy():
    c = psycopg2.connect(DSN, connect_timeout=2)
    c.autocommit = True
    cur = c.cursor()
    cur.execute("SELECT 1 + 1")
    r = cur.fetchone()
    c.close()
    return r == (2,)


def phase(name, fn, p):
    try:
        fn()
    except Exception as e:
        print(f"  [FAIL] {name}: probe itself raised {type(e).__name__}: {e}")
        FAILS.append(name)
        return
    time.sleep(0.2)
    if not alive(p):
        print(f"  [FAIL] {name}: SERVER DIED")
        FAILS.append(name)
        return
    try:
        if not healthy():
            print(f"  [FAIL] {name}: server alive but not serving")
            FAILS.append(name)
            return
    except Exception as e:
        print(f"  [FAIL] {name}: health check failed: {str(e)[:60]}")
        FAILS.append(name)
        return
    print(f"  [OK] {name}")


def send_raw(data, then=b"", read_reply=False):
    s = socket.create_connection(("127.0.0.1", PORT), timeout=3)
    try:
        s.sendall(data)
        if then:
            time.sleep(0.05)
            s.sendall(then)
        if read_reply:
            s.settimeout(1.0)
            try:
                return s.recv(4096)
            except socket.timeout:
                return b""
    finally:
        s.close()
    return b""


def startup_body(user=b"u", db=b"x", prot=196608):
    b = struct.pack(">I", prot) + b"\x00" + b"user\x00" + user + b"\x00" + \
        b"database\x00" + db + b"\x00\x00"
    return b


def pkt(kind, body):
    k = bytes([kind]) if isinstance(kind, int) else kind[:1]
    return k + struct.pack(">I", len(body) + 4) + body


def f_random_garbage():
    for n in (1, 7, 100, 4096, 65536):
        send_raw(os.urandom(n))


def f_truncated_startup():
    b = startup_body()
    send_raw(struct.pack(">I", len(b) + 4) + b[:len(b) // 2])


def f_huge_length():
    # length prefix says 2GB; body is 3 bytes; then close
    send_raw(b"\x00" + struct.pack(">I", 0x7FFF0000) + b"abc")
    send_raw(b"\x00" + struct.pack(">I", 0xFFFFFFFF)[:4] + b"xyz")


def f_zero_and_tiny_length():
    send_raw(b"\x00" + struct.pack(">I", 0))
    send_raw(b"\x00" + struct.pack(">I", 3))
    send_raw(b"\x00" + struct.pack(">I", 4))  # valid len, empty body, no more


def f_sslrequest_then_garbage():
    send_raw(struct.pack(">I", 8) + struct.pack(">I", 80877103))
    send_raw(struct.pack(">I", 8) + struct.pack(">I", 80877103) + os.urandom(64))
    # SSLRequest followed by a bogus second message on same socket
    s = socket.create_connection(("127.0.0.1", PORT), timeout=3)
    s.sendall(struct.pack(">I", 8) + struct.pack(">I", 80877103))
    time.sleep(0.1)
    try:
        s.recv(16)  # 'N'
        s.sendall(b"\xff" * 100)
    except OSError:
        pass
    s.close()


def f_garbage_after_startup():
    # complete startup, then random kinds and bodies
    for _ in range(20):
        body = startup_body()
        s = socket.create_connection(("127.0.0.1", PORT), timeout=3)
        s.sendall(struct.pack(">I", len(body) + 4) + body)
        time.sleep(0.05)
        kind = random.randrange(256)
        s.sendall(pkt(kind, os.urandom(random.randrange(1, 200))))
        try:
            s.settimeout(0.5)
            s.recv(4096)
        except OSError:
            pass
        s.close()


def f_query_garbage_body():
    body = startup_body()
    s = socket.create_connection(("127.0.0.1", PORT), timeout=3)
    s.sendall(struct.pack(">I", len(body) + 4) + body)
    time.sleep(0.1)
    try:
        s.recv(4096)  # auth ok / params
    except OSError:
        pass
    s.sendall(pkt(b"Q", os.urandom(64)))  # not a C-string, binary junk
    try:
        s.settimeout(1.0)
        s.recv(4096)  # expect an ErrorResponse eventually
    except OSError:
        pass
    s.close()


def f_mid_packet_disconnect():
    for _ in range(10):
        s = socket.create_connection(("127.0.0.1", PORT), timeout=3)
        s.sendall(pkt(b"Q", b"SELECT ")[:8])  # half a query
        s.close()
    # slowloris-lite: byte at a time, then abandon
    s = socket.create_connection(("127.0.0.1", PORT), timeout=3)
    body = startup_body()
    msg = struct.pack(">I", len(body) + 4) + body
    for byte in msg[:20]:
        s.sendall(bytes([byte]))
        time.sleep(0.02)
    s.close()


def f_rapid_garbage_conns():
    def one():
        try:
            s = socket.create_connection(("127.0.0.1", PORT), timeout=3)
            s.sendall(os.urandom(random.randrange(1, 400)))
            s.close()
        except OSError:
            pass
    ths = [__import__("threading").Thread(target=one) for _ in range(100)]
    for t in ths:
        t.start()
    for t in ths:
        t.join()


def f_terminate_and_reuse():
    body = startup_body()
    s = socket.create_connection(("127.0.0.1", PORT), timeout=3)
    s.sendall(struct.pack(">I", len(body) + 4) + body)
    time.sleep(0.05)
    s.recv(4096)
    s.sendall(pkt(b"X", b""))  # Terminate
    s.close()


def main():
    p = start()
    cur = psycopg2.connect(DSN)
    cur.autocommit = True
    c = cur.cursor()
    c.execute("CREATE TABLE wire_keep (v int)")
    c.execute("INSERT INTO wire_keep VALUES (42)")
    cur.close()

    print("wire resilience probes:")
    phase("random garbage (1B..64KB)", f_random_garbage, p)
    phase("truncated startup message", f_truncated_startup, p)
    phase("huge length prefix lies", f_huge_length, p)
    phase("zero/tiny/empty lengths", f_zero_and_tiny_length, p)
    phase("SSLRequest + garbage", f_sslrequest_then_garbage, p)
    phase("garbage kinds after startup", f_garbage_after_startup, p)
    phase("Query with binary garbage body", f_query_garbage_body, p)
    phase("mid-packet disconnect + slowloris-lite", f_mid_packet_disconnect, p)
    phase("100 rapid garbage connections", f_rapid_garbage_conns, p)
    phase("Terminate and reuse", f_terminate_and_reuse, p)

    # final: data written before the abuse must be intact
    c = psycopg2.connect(DSN)
    c.autocommit = True
    cur = c.cursor()
    cur.execute("SELECT v FROM wire_keep")
    r = cur.fetchone()
    c.close()
    if r != (42,):
        FAILS.append("data intact after abuse")
        print(f"  [FAIL] data after abuse: {r}")
    else:
        print("  [OK] data intact after abuse")

    os.kill(p.pid, 9)
    p.wait()
    if FAILS:
        print(f"WIRE TESTS FAILED: {FAILS}")
        sys.exit(1)
    print("WIRE TESTS PASSED (server survived all hostile input and kept "
          "serving)")


if __name__ == "__main__":
    main()
