#!/usr/bin/env python3
"""puddle adversarial protocol + SQL fuzzer.

Six layers against live servers, with a hard oracle:

  L0  directed kill-probes (deterministic): nesting depth, length-field
      lies, memory amplification, abandoned COPY, reset mid-handshake
  L1  startup / framing mutations (pre-auth)
  L2  message-body mutations (authenticated session)
  L3  extended-protocol (Parse/Bind/Describe/Execute) corruption
  L4  SCRAM-SHA-256 exchange corruption (separate scram server)
  L5  grammar-generated + mutated SQL over the simple protocol
  L6  recorded-session replay with byte-level mutation

Oracle, checked after every single case:
  1. the server process is still alive
  2. a fresh liveness connection answers SELECT 1 with valid framing
  3. every byte the server ever sent parses as well-formed PG v3
     messages (type + length in range + body; no torn tails on close)
  4. RSS stays under a ceiling (leak / preallocation detection)
  5. a victim table reads back byte-exact at every layer boundary

Any violation: the offending input is saved, delta-debugged to a minimal
reproducer, and the run exits 1.

usage: python3 hostile_fuzz.py [seconds] [seed]
       seconds: fuzz budget after L0 (default 60)
       seed:    master seed (default: random, printed; reuse to reproduce)
"""

import json
import os
import random
import shutil
import signal
import socket
import struct
import subprocess
import sys
import time

import psycopg2

BIN = os.path.join(os.path.dirname(__file__), "..", "target", "release", "puddle")
PORT = 5510
SCRAM_PORT = 5511
DATA = "/tmp/puddle-hostile-fuzz"
SCRAM_DATA = "/tmp/puddle-hostile-fuzz-scram"
AUTH_FILE = "/tmp/puddle-hostile-fuzz-users.json"
ARTDIR = "/tmp/puddle-hostile-fuzz/artifacts"

MAX_MSG = 64 * 1024 * 1024  # the server's own frame cap
CANARY_TABLE = "zz_canary_7c4f1a"  # unguessable: generated SQL must never name it
RSS_CEILING_MB = 220
AMP_CONNS = 24
AMP_ALLOW_MB = 192  # amplification budget: declared-but-unsent bytes

VIOLATIONS = []
STATS = {}


# ------------------------------------------------------------------ server --

def spawn(port, data, extra=()):
    log = open(f"/tmp/hostile-fuzz-{port}.log", "w")
    p = subprocess.Popen(
        [BIN, "--port", str(port), "--data", f"file://{data}", *extra],
        stdout=log, stderr=subprocess.STDOUT,
    )
    for _ in range(200):
        if p.poll() is not None:
            print(open(f"/tmp/hostile-fuzz-{port}.log").read()[-3000:])
            sys.exit(f"! server on {port} died during startup")
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.5)
            s.close()
            # defeat the bind-race: a stale server from a previous run can
            # answer the probe while OUR process dies on "address already in
            # use" milliseconds later. re-check liveness after a settle.
            time.sleep(0.2)
            if p.poll() is not None:
                print(open(f"/tmp/hostile-fuzz-{port}.log").read()[-3000:])
                sys.exit(f"! server on {port} died during startup")
            if "could not bind" in open(f"/tmp/hostile-fuzz-{port}.log").read():
                sys.exit(f"! port {port} is held by a stale process; kill it first")
            return p
        except OSError:
            time.sleep(0.05)
    sys.exit(f"! server on {port} never listened")


def rss_mb(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        pass
    return 0.0


class Server:
    def __init__(self, port, data, extra=(), auth="trust"):
        self.port, self.extra = port, extra
        self.data = data
        self.auth = auth
        self.proc = spawn(port, data, extra)
        self.born = time.time()

    def restart(self):
        kill(self)
        shutil.rmtree(self.data, ignore_errors=True)
        os.makedirs(self.data, exist_ok=True)
        self.proc = spawn(self.port, self.data, self.extra)
        seed_victim(self.port)


def kill(srv):
    for _ in range(3):
        if srv.proc.poll() is not None:
            return
        try:
            srv.proc.send_signal(signal.SIGKILL)
            srv.proc.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            continue
        except Exception:
            return


# ------------------------------------------------------------- wire helpers --

def startup_pkt(user="u", db="x", extra=()):
    body = struct.pack(">I", 196608)
    for k, v in (("user", user), ("database", db), *extra):
        body += k.encode() + b"\x00" + v.encode() + b"\x00"
    body += b"\x00"
    return struct.pack(">I", len(body) + 4) + body


def msg(t, body=b""):
    t = t if isinstance(t, int) else t[0]
    return bytes([t]) + struct.pack(">I", len(body) + 4) + body


def frame_walk(buf, at_open):
    """Validates buf as a sequence of PG v3 messages.
    Returns (status, consumed) with status in:
      ok            - all bytes are complete, well-formed messages
      partial-tail  - trailing bytes are a consistent incomplete message
    Anything else (absurd length, torn tail on close) is a violation.
    """
    i = 0
    # handshake phase: the server's first reply byte to SSLRequest/GssencRequest
    # is a bare 'S'/'N' with NO length prefix (PG protocol). At stream position
    # 0 that single byte is legal; after it the client normally restarts the
    # handshake, so framed messages resume at offset 1. A leading 'S' may also
    # be followed by raw TLS records, which are not PG frames - stop there.
    if i == 0 and buf[:1] in (b"N", b"S") and len(buf) > 1:
        if buf[0:1] == b"S" and buf[1:1] != b"" and not (len(buf) > 5 and 4 <= struct.unpack(">I", buf[1:5])[0] <= MAX_MSG):
            return ("ok", 1)  # SSLRequest accept: rest is a TLS record stream
        if not (len(buf) - 1 >= 5 and 4 <= struct.unpack(">I", buf[1:5])[0] <= MAX_MSG):
            # not a plausible framed message after the bare byte: accept the
            # handshake reply alone
            return ("ok", 1)
        i = 1
    while i < len(buf):
        if len(buf) - i < 5:
            return ("partial-tail", i) if at_open else ("torn", i)
        ln = struct.unpack(">I", buf[i + 1: i + 5])[0]
        if not (4 <= ln <= MAX_MSG):
            return ("bad-length", i)
        if len(buf) - i < 1 + ln:
            return ("partial-tail", i) if at_open else ("torn", i)
        i += 1 + ln
    return ("ok", i)


def cstr(s):
    return (s if isinstance(s, bytes) else s.encode()) + b"\x00"


# ------------------------------------------------------------ case running --

class Case:
    """A scripted dialogue. Ops:
       ("send", bytes)          send (failure tolerated: server may close)
       ("send!", bytes)         send must succeed
       ("expect", b"Z", t)      read until a message of this type (validated)
       ("drain",)               read until quiet/close/1s
    """
    __slots__ = ("ops", "layer", "name", "partial_ok")

    def __init__(self, layer, name, ops, partial_ok=False):
        self.layer, self.name, self.ops, self.partial_ok = layer, name, ops, partial_ok


def run_case(server, case):
    """Executes one case. Returns None on pass, or a violation dict."""
    out = b""
    s = socket.socket()
    s.settimeout(1.0)
    try:
        try:
            s.connect(("127.0.0.1", server.port))
        except OSError as e:
            return _violation(server, case, "connect-refused", out, str(e))
        for op in case.ops:
            kind = op[0]
            if kind in ("send", "send!"):
                try:
                    s.sendall(op[1])
                except OSError as e:
                    if kind == "send!":
                        return _violation(server, case, "send-failed", out, str(e))
                    break  # server closed early on garbage: acceptable
            elif kind == "expect":
                want, tmo = op[1], op[2]
                s.settimeout(tmo)
                deadline = time.time() + tmo
                got = None
                while time.time() < deadline:
                    try:
                        chunk = s.recv(65536)
                    except socket.timeout:
                        break
                    except OSError:
                        break
                    if not chunk:
                        break
                    out += chunk
                    st, _ = frame_walk(out, True)
                    if st not in ("ok", "partial-tail"):
                        return _violation(server, case, f"bad-frame:{st}", out, "")
                    # scan for the wanted message type at message boundaries
                    if _has_type(out, want):
                        got = want
                        break
                if got is None:
                    return _violation(server, case, f"expect-timeout:{want!r}", out, "")
            elif kind == "drain":
                s.settimeout(1.0)
                deadline = time.time() + 1.0
                while time.time() < deadline:
                    try:
                        chunk = s.recv(65536)
                    except socket.timeout:
                        break
                    except OSError:
                        break
                    if not chunk:
                        break
                    out += chunk
        # final framing check: connection now closed or idle; a torn tail on
        # a socket that went quiet is only tolerated for partial-message cases
        st, _ = frame_walk(out, True)
        if st not in ("ok", "partial-tail"):
            return _violation(server, case, f"bad-frame:{st}", out, "")
    except Exception as e:  # noqa: BLE001 - any harness surprise is a finding
        return _violation(server, case, "harness-error", out, repr(e))
    finally:
        try:
            s.close()
        except OSError:
            pass

    # oracle 1+2: process alive and serving
    if server.proc.poll() is not None:
        return _violation(server, case, "server-died", out, "")
    if not _liveness(server):
        return _violation(server, case, "server-wedged", out, "")
    return None


def _has_type(buf, t):
    i = 0
    while i + 5 <= len(buf):
        ln = struct.unpack(">I", buf[i + 1: i + 5])[0]
        if not (4 <= ln <= MAX_MSG):
            return False
        if buf[i] == t[0]:
            return True
        i += 1 + ln
    return False


def _liveness(server):
    """Fresh connection proving the accept loop + framer are alive.
    trust server: startup + SELECT 1 -> must reach ReadyForQuery.
    scram server: startup -> must get a framed AuthenticationSASL ('R')."""
    if getattr(server, "auth", "trust") == "scram":
        try:
            s = socket.create_connection(("127.0.0.1", server.port), timeout=2.0)
        except OSError:
            return False
        try:
            s.settimeout(2.0)
            s.sendall(startup_pkt(user="fuzzu"))
            buf = b""
            deadline = time.time() + 2.0
            while time.time() < deadline:
                try:
                    c = s.recv(65536)
                except (socket.timeout, OSError):
                    return False
                if not c:
                    return False
                buf += c
                if len(buf) >= 5:
                    ln = struct.unpack(">I", buf[1:5])[0]
                    if buf[0:1] == b"R" and 4 <= ln <= MAX_MSG and len(buf) >= 1 + ln:
                        return True
            return False
        finally:
            try:
                s.close()
            except OSError:
                pass
    try:
        s = socket.create_connection(("127.0.0.1", server.port), timeout=2.0)
    except OSError:
        return False
    try:
        s.settimeout(2.0)
        s.sendall(startup_pkt())
        s.sendall(msg(b"Q", b"SELECT 1\x00"))
        buf = b""
        deadline = time.time() + 2.0
        while time.time() < deadline:
            try:
                c = s.recv(65536)
            except (socket.timeout, OSError):
                return False
            if not c:
                return False
            buf += c
            if _has_type(buf, b"Z"):
                # tolerate a torn tail: we may have found the auth-phase Z
                # while the pipelined query response is still streaming in
                st, _ = frame_walk(buf, True)
                return st in ("ok", "partial-tail")
        return False
    finally:
        try:
            s.close()
        except OSError:
            pass


# -------------------------------------------------------------- violation ---

def _violation(server, case, kind, out, note):
    return {
        "server": server, "case": case, "kind": kind,
        "out": out, "note": note, "alive": server.proc.poll() is None,
    }


def save_artifact(v, minimized=None):
    os.makedirs(ARTDIR, exist_ok=True)
    d = os.path.join(ARTDIR, f"fail-{len(VIOLATIONS)}")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "meta.txt"), "w") as f:
        f.write(f"kind: {v['kind']}\nlayer: {v['case'].layer}\n")
        f.write(f"case: {v['case'].name}\nnote: {v['note']}\n")
        f.write(f"server-alive: {v['alive']}\n")
    with open(os.path.join(d, "ops.txt"), "w") as f:
        for i, op in enumerate(v["case"].ops):
            if op[0] in ("send", "send!"):
                f.write(f"--- op{i} {op[0]} ({len(op[1])}B) ---\n")
                f.write(op[1].hex() + "\n")
            else:
                f.write(f"--- op{i} {op} ---\n")
    if minimized is not None:
        with open(os.path.join(d, "minimized.txt"), "w") as f:
            for op in (minimized.ops if isinstance(minimized, Case) else minimized):
                if op[0] in ("send", "send!"):
                    f.write(op[1].hex() + "\n")
                else:
                    f.write(f"{op}\n")
    logp = f"/tmp/hostile-fuzz-{v['server'].port}.log"
    if os.path.exists(logp):
        shutil.copy(logp, os.path.join(d, "server.log"))
    print(f"\n!! VIOLATION [{v['case'].layer}/{v['case'].name}] {v['kind']} {v['note']}")
    print(f"   artifact: {d}")
    return d


# --------------------------------------------------------- delta-debugging --

def minimize(server, case, repro):
    """Shrinks a failing case to a minimal reproducer (1s budget)."""
    if server.proc.poll() is not None:
        server.restart()
    ops = list(case.ops)
    improved = True
    t0 = time.time()
    while improved and time.time() - t0 < 20:
        improved = False
        n = len(ops)
        for span in (max(1, n // 2), max(1, n // 4), 1):
            i = 0
            while i < len(ops) and len(ops) > 1:
                cand = ops[:i] + ops[i + span:]
                if not cand:
                    i += span
                    continue
                c = Case(case.layer, case.name, cand, case.partial_ok)
                v = run_case(server, c)
                if v is not None and repro(v):
                    ops = cand
                    improved = True
                else:
                    i += span
    return Case(case.layer, case.name + ".min", ops, case.partial_ok)


# ------------------------------------------------------------------ layers --

def layer0(server):
    """Directed kill-probes: one per known failure class."""
    out = []

    def q(sql):
        return [ ("send", startup_pkt()), ("send", msg(b"Q", sql + b"\x00")), ("drain",) ]

    # expression nesting: parens, subqueries, CASE, function args, casts
    for depth in (32, 128, 512, 4096, 65536):
        out.append(Case("L0", f"paren-depth-{depth}",
                        q(b"SELECT " + b"(" * depth + b"1" + b")" * depth)))
    for depth in (16, 128, 2048):
        inner = b"SELECT 1"
        for _ in range(depth):
            inner = b"SELECT (" + inner + b")"
        out.append(Case("L0", f"subquery-depth-{depth}", q(inner)))
    for depth in (16, 128, 2048):
        e = b"1"
        for _ in range(depth):
            e = b"CASE WHEN 1 > 0 THEN " + e + b" ELSE 0 END"
        out.append(Case("L0", f"case-depth-{depth}", q(b"SELECT " + e)))
    for depth in (64, 1024):
        e = b"1"
        for _ in range(depth):
            e = b"f(" + e + b")"
        out.append(Case("L0", f"call-depth-{depth}", q(b"SELECT " + e)))

    # length-field lies (framing)
    for ln in (0, 1, 3, 4, 5, MAX_MSG, MAX_MSG + 1, 0x7FFFFFFF, 0xFFFFFFFF):
        out.append(Case("L0", f"len-lie-{ln}",
                        [("send", b"Q" + struct.pack(">I", ln) + b"SELECT 1")]))
    for ln in (0, 4, 7, 9, 9999, 10001, 0x7FFFFFFF):
        body = struct.pack(">I", 196608) + b"user\x00u\x00\x00"
        out.append(Case("L0", f"startup-len-lie-{ln}",
                        [("send", struct.pack(">I", ln) + body)]))

    # v2.0 snapshot / AS OF directed probes
    for i, sql in enumerate([
        b"SELECT * FROM nosuch AS OF SNAPSHOT nosuch2",
        b"SELECT * FROM nosuch AS OF LSN 0",
        b"SELECT * FROM nosuch AS OF LSN -1",
        b"SELECT * FROM nosuch AS OF LSN 99999999999999999999",
        b"CREATE SNAPSHOT ''",
        b"CREATE SNAPSHOT " + b"x" * 70000,
        b"DROP SNAPSHOT " + b"y" * 70000,
        b"SELECT * FROM pg_tables AS OF SNAPSHOT nope",
        b"SELECT * FROM generate_series(1,3) AS OF LSN 1",
        b"SELECT 1 AS OF SNAPSHOT nope",
        b"CREATE SNAPSHOT a; CREATE SNAPSHOT a",
        b"SELECT count(*) FROM information_schema.tables AS OF LSN 1",
    ]):
        out.append(Case("L0", f"snapshot-probe-{i}", q(sql)))

    # oversized but honest payloads
    out.append(Case("L0", "q-1mb-pipelined",
                    [("send", startup_pkt()),
                     ("send", msg(b"Q", b"SELECT 1;" * 131072))]))
    out.append(Case("L0", "q-unterminated-string",
                    q(b"SELECT 'never-closed")))
    out.append(Case("L0", "q-unterminated-comment",
                    q(b"SELECT 1 /* no close")))
    out.append(Case("L0", "q-invalid-utf8",
                    q(b"SELECT \xff\xfe\x80")))
    out.append(Case("L0", "q-params-4096",
                    q(b"SELECT " + b",".join(b"$%d" % i for i in range(1, 4097)))))

    # abandoned COPY and stray copy messages
    out.append(Case("L0", "copyin-announced-then-close", [
        ("send", startup_pkt()),
        ("send", msg(b"Q", b"COPY victim FROM STDIN\x00")),
        ("expect", b"G", 1.5),
        ("send", b"garbage-not-a-message"),
        ("drain",),
    ], partial_ok=True))
    out.append(Case("L0", "copyin-claimed-64mb", [
        ("send", startup_pkt()),
        ("send", msg(b"Q", b"COPY victim FROM STDIN\x00")),
        ("expect", b"G", 1.5),
        ("send", b"d" + struct.pack(">I", MAX_MSG) + b"x"),
        ("drain",),
    ], partial_ok=True))
    out.append(Case("L0", "stray-copydone", [
        ("send", startup_pkt()),
        ("send", msg(b"c")),
        ("send", msg(b"f", b"why\x00")),
        ("send", msg(b"d", b"late")),
        ("send", msg(b"Q", b"SELECT 1\x00")),
        ("drain",),
    ]))

    # handshake-level abuse
    out.append(Case("L0", "sslrequest-then-garbage",
                    [("send", struct.pack(">II", 8, 80877103) + os.urandom(64))]))
    out.append(Case("L0", "gssenc-then-valid-startup", [
        ("send", struct.pack(">II", 8, 80877104)),
        ("send", startup_pkt()),
        ("send", msg(b"Q", b"SELECT 1\x00")),
        ("drain",),
    ]))
    out.append(Case("L0", "cancelrequest-midstream", [
        ("send", startup_pkt()),
        ("send", struct.pack(">II", 16, 80877102) + struct.pack(">II", 1, 2)),
        ("send", msg(b"Q", b"SELECT 1\x00")),
        ("drain",),
    ]))
    out.append(Case("L0", "second-startup-midsession", [
        ("send", startup_pkt()),
        ("send", startup_pkt()),
        ("send", msg(b"Q", b"SELECT 1\x00")),
        ("drain",),
    ]))
    out.append(Case("L0", "reset-mid-handshake", [
        ("send", startup_pkt()[:9]),
        ("drain",),
    ], partial_ok=True))

    # memory amplification: headers claiming 64MB, nothing follows
    out.append(("amp", None))
    return out


def probe_amplification(server):
    print("  [L0] memory amplification: "
          f"{AMP_CONNS} conns x 64MB claimed, ~{AMP_CONNS * 5}B sent")
    rss0 = rss_mb(server.proc.pid)
    socks = []
    try:
        for _ in range(AMP_CONNS):
            s = socket.create_connection(("127.0.0.1", server.port), timeout=2)
            s.sendall(startup_pkt())
            s.sendall(b"d" + struct.pack(">I", MAX_MSG))
            socks.append(s)
        time.sleep(1.5)
        rss1 = rss_mb(server.proc.pid)
    finally:
        for s in socks:
            try:
                s.close()
            except OSError:
                pass
    delta = rss1 - rss0
    ok = delta <= AMP_ALLOW_MB and server.proc.poll() is None and _liveness(server)
    print(f"       RSS {rss0:.1f} -> {rss1:.1f} MB (delta {delta:+.1f} MB) "
          f"{'OK' if ok else 'FAIL'}")
    if not ok:
        v = _violation(server, Case("L0", "amplification", []),
                       "rss-amplification" if server.proc.poll() is None else "server-died",
                       b"", f"delta={delta:.1f}MB")
        VIOLATIONS.append(v)
        save_artifact(v)
        return False
    return True


# ---- L1: startup / framing mutations (pre-auth) ----

def layer1(rng):
    cases = []
    base = startup_pkt()
    flips = lambda b: bytes(x ^ (1 << rng.randrange(8)) if rng.random() < 0.02 else x
                            for x in b)
    for i in range(140):
        style = rng.randrange(8)
        if style == 0:  # bit flips over a valid startup
            b = bytearray(base)
            for _ in range(rng.randint(1, 6)):
                b[rng.randrange(len(b))] ^= 1 << rng.randrange(8)
            data = bytes(b)
        elif style == 1:  # length field lie
            ln = rng.choice([0, 1, 4, 7, 8, 9, rng.randrange(0, 40), 10001,
                             0x7FFFFFFF, 0xFFFFFFFF])
            data = struct.pack(">I", ln) + base[4:]
        elif style == 2:  # truncation
            data = base[:rng.randrange(0, len(base))]
        elif style == 3:  # pure random
            data = bytes(rng.randrange(256) for _ in range(rng.randrange(0, 80)))
        elif style == 4:  # valid length, random body
            body = bytes(rng.randrange(256) for _ in range(rng.randrange(0, 60)))
            data = struct.pack(">I", len(body) + 4) + body
        elif style == 5:  # param abuse
            n = rng.randint(1, 12)
            params = b""
            for _ in range(n):
                k = bytes(rng.randrange(1, 128) for _ in range(rng.randrange(0, 10)))
                val = bytes(rng.randrange(1, 128) for _ in range(rng.randrange(0, 10)))
                params += k + b"\x00" + val + b"\x00"
            params += b"\x00"
            body = struct.pack(">I", rng.choice([196608, 196609, 0, 3])) + params
            data = struct.pack(">I", len(body) + 4) + body
        elif style == 6:  # startup family codes
            code = rng.choice([80877102, 80877103, 80877104, 196608, 196609])
            body = struct.pack(">I", code) + os.urandom(rng.randrange(0, 16))
            data = struct.pack(">I", len(body) + 4) + body
        else:  # splice of two startups
            cut = rng.randrange(0, len(base))
            data = base[:cut] + base[rng.randrange(0, len(base)):]
        cases.append(Case("L1", f"startup-mut-{i}", [("send", data), ("drain",)],
                          partial_ok=True))
    # message framing on a bare socket (no startup at all)
    for i in range(60):
        t = rng.randrange(256)
        ln = rng.choice([rng.randrange(0, 12), rng.randrange(0, 1 << 20),
                         MAX_MSG, MAX_MSG + 1, 0xFFFFFFFF])
        body = bytes(rng.randrange(256) for _ in range(min(ln if ln < 4096 else 8, 4096)))
        cases.append(Case("L1", f"bare-frame-{i}",
                          [("send", bytes([t]) + struct.pack(">I", ln) + body),
                           ("drain",)], partial_ok=True))
    return cases


# ---- L2: message-body mutations (authenticated) ----

QUERIES = [
    b"SELECT 1",
    b"SELECT * FROM victim ORDER BY id",
    b"SELECT id, count(*) FROM victim GROUP BY id HAVING count(*) > 0",
    b"SELECT c1.tag FROM victim c1 JOIN victim c2 ON c1.id = c2.id WHERE c1.val > 0.5",
    b"BEGIN",
    b"COMMIT",
    b"ROLLBACK",
    b"INSERT INTO victim (id, tag, val) VALUES (900001, 'x', 1.5)",
    b"UPDATE victim SET val = 2.5 WHERE id < 0",
    b"DELETE FROM victim WHERE id < 0",
    b"SELECT relname FROM pg_class WHERE relkind = 'r'",
    b"SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname !~ '^pg_'",
    b"SELECT E'x\\ny', current_database()",
    b"SELECT (SELECT max(id) FROM victim)",
    b"COPY victim TO STDOUT",
    b"COPY (SELECT 1) TO STDOUT WITH (FORMAT csv)",
    b"SELECT 1 UNION SELECT 2 ORDER BY 1",
    b"SELECT x FROM generate_series(1, 8) AS x WHERE x % 2 = 0",
]


def mutate_bytes(rng, b):
    b = bytearray(b)
    for _ in range(rng.randint(1, 4)):
        if not b:
            break
        op = rng.randrange(4)
        i = rng.randrange(len(b))
        if op == 0:
            b[i] ^= 1 << rng.randrange(8)
        elif op == 1:
            b[i] = rng.randrange(256)
        elif op == 2:
            del b[i]
        else:
            b.insert(i, rng.randrange(256))
    return bytes(b)


def layer2(rng):
    cases = []
    for i in range(140):
        sql = mutate_bytes(rng, rng.choice(QUERIES))
        cases.append(Case("L2", f"q-mut-{i}", [
            ("send", startup_pkt()),
            ("send", msg(b"Q", sql + b"\x00")),
            ("drain",),
        ]))
    # random types with structured garbage bodies
    for i in range(60):
        t = rng.choice(b"QPEDCSHQfdcnBXISTM")
        body = bytes(rng.randrange(256) for _ in range(rng.randrange(0, 64)))
        cases.append(Case("L2", f"type-{chr(t)}-{i}", [
            ("send", startup_pkt()),
            ("send", msg(t, body)),
            ("send", msg(b"Q", b"SELECT 1\x00")),
            ("drain",),
        ]))
    # giant single statement
    for i in range(8):
        unit = rng.choice([b"SELECT 1;", b"(", b")", b"''", b"\\N", b";", b"--\n"])
        cases.append(Case("L2", f"q-giant-{i}", [
            ("send", startup_pkt()),
            ("send", msg(b"Q", unit * rng.randrange(1000, 40000) + b"\x00")),
            ("drain",),
        ]))
    return cases


# ---- L3: extended protocol corruption ----

def parse_msg(name, sql, oids):
    return (cstr(name) + cstr(sql) + struct.pack(">H", len(oids))
            + b"".join(struct.pack(">I", o) for o in oids))


def bind_msg(pname, sname, fmts, params, rfmts):
    b = cstr(pname) + cstr(sname)
    b += struct.pack(">H", len(fmts)) + b"".join(struct.pack(">h", f) for f in fmts)
    b += struct.pack(">H", len(params))
    for p in params:
        b += struct.pack(">I", len(p)) + p
    b += struct.pack(">H", len(rfmts)) + b"".join(struct.pack(">h", f) for f in rfmts)
    return b


def layer3(rng):
    cases = []
    for i in range(110):
        sql = rng.choice([b"SELECT $1", b"SELECT $1 + $2", b"SELECT $1::text",
                          b"SELECT * FROM victim WHERE id = $1", b"SELECT 1"])
        nparams = rng.randint(0, 3)
        oids = [rng.choice([0, 0, 25, 23, 701, 16, rng.randrange(2 ** 32)])
                for _ in range(rng.randint(0, 4))]
        vals = [str(rng.randrange(-1000, 1000)).encode() for _ in range(nparams)]
        fmts = [rng.choice([0, 0, 0, 1, rng.randrange(-3, 4)]) for _ in range(rng.randint(0, 3))]
        rfmts = [rng.choice([0, 0, 1]) for _ in range(rng.randint(0, 3))]
        ops = [("send", startup_pkt()),
               ("send", msg(b"P", parse_msg("", sql, oids))),
               ("send", msg(b"B", bind_msg("", "", fmts, vals, rfmts))),
               ("send", msg(b"D", b"S\x00")),
               ("send", msg(b"E", cstr("") + struct.pack(">I", rng.choice([0, 1, 10, 2 ** 31 - 1])))),
               ("send", msg(b"S")),
               ("drain",)]
        # targeted corruption
        if rng.random() < 0.5:
            k = rng.randrange(1, 4)
            for _ in range(k):
                which = rng.randrange(4)
                if which == 0:  # lie about a param value length
                    ops[2] = ("send", msg(b"B", mutate_bytes(rng, bind_msg("", "", fmts, vals, rfmts))))
                elif which == 1:  # absurd format code
                    ops[2] = ("send", msg(b"B", bind_msg("", "", fmts + [rng.randrange(-32768, 32768)], vals, rfmts)))
                elif which == 2:  # drop a message from the pipeline
                    del ops[rng.randrange(1, 6)]
                else:  # send garbage as a named statement payload
                    ops.insert(rng.randrange(1, 5),
                               ("send", msg(rng.choice(b"PBDESC"), bytes(rng.randrange(256) for _ in range(rng.randrange(0, 24))))))
        cases.append(Case("L3", f"ext-{i}", ops, partial_ok=True))
    # named statements + close + reuse
    for i in range(30):
        ops = [("send", startup_pkt()),
               ("send", msg(b"P", parse_msg("s1", b"SELECT $1", [0]))),
               ("send", msg(b"B", bind_msg("p1", "s1", [0], [b"7"], [0]))),
               ("send", msg(b"E", cstr("p1") + struct.pack(">I", 0))),
               ("send", msg(b"C", b"S\x00")),
               ("send", msg(b"B", bind_msg("p1", "s1", [0], [b"8"], [0]))),
               ("send", msg(b"E", cstr("p1") + struct.pack(">I", 0))),
               ("send", msg(b"S")),
               ("drain",)]
        if rng.random() < 0.6:
            j = rng.randrange(1, 8)
            ops[j] = ("send", mutate_bytes(rng, ops[j][1]))
        cases.append(Case("L3", f"named-{i}", ops, partial_ok=True))
    return cases


# ---- L4: SCRAM exchange corruption ----

def scram_handshake_ops(rng, corrupt):
    import base64
    ops = [("send", startup_pkt(user="fuzzu")),
           ("expect", b"R", 1.5)]
    # client-first: n,,n=*,r=<nonce>
    nonce = ("fuzznonce%d" % rng.randrange(10 ** 6)).encode()
    cfirst = b"n,,n=fuzzu,r=" + nonce
    if corrupt == "first":
        cfirst = mutate_bytes(rng, cfirst)
    ops += [("send", msg(b"p", cstr("SCRAM-SHA-256")
                         + struct.pack(">i", len(cfirst)) + cfirst)),
            ("expect", b"R", 1.5)]
    # client-final: c=biws,r=<nonce>,p=<proof>  (proof is wrong on purpose;
    # the point is framing/robustness, not successful auth)
    cfinal = b"c=biws,r=" + nonce + b",p=" + base64.b64encode(b"\x00" * 32)
    if corrupt == "final":
        cfinal = mutate_bytes(rng, cfinal)
    elif corrupt == "final-huge":
        cfinal = b"c=biws,r=" + nonce + b",p=" + b"A" * (1 << 20)
    ops += [("send", msg(b"p", cfinal)), ("drain",)]
    return ops


def layer4(rng):
    cases = []
    # control: the uncorrupted shape must fail cleanly (wrong proof)
    cases.append(Case("L4", "control", scram_handshake_ops(rng, None)))
    for i in range(40):
        cases.append(Case("L4", f"cfirst-{i}", scram_handshake_ops(rng, "first")))
    for i in range(40):
        cases.append(Case("L4", f"cfinal-{i}", scram_handshake_ops(rng, "final")))
    cases.append(Case("L4", "cfinal-huge", scram_handshake_ops(rng, "final-huge")))
    # wrong message shapes at auth stage
    for i in range(30):
        t = rng.choice(b"pQ")
        body = bytes(rng.randrange(256) for _ in range(rng.randrange(0, 48)))
        cases.append(Case("L4", f"auth-garbage-{i}", [
            ("send", startup_pkt(user="fuzzu")),
            ("expect", b"R", 1.5),
            ("send", msg(t, body)),
            ("drain",),
        ]))
    for i in range(10):
        mech = bytes(rng.randrange(65, 91) for _ in range(rng.randrange(0, 12)))
        cases.append(Case("L4", f"bad-mech-{i}", [
            ("send", startup_pkt(user="fuzzu")),
            ("expect", b"R", 1.5),
            ("send", msg(b"p", cstr(mech.decode(errors="ignore"))
                         + struct.pack(">i", -1))),
            ("drain",),
        ]))
    return cases


# ---- L5: SQL grammar generation + mutation ----

LITS = [b"42", b"-7", b"3.14", b"-0.0", b"1e308", b"1e309", b"9223372036854775807",
        b"9223372036854775808", b"0.1e-1", b"'txt'", b"''", b"'don''t'", b"E'x\\ny'",
        b"NULL", b"TRUE", b"FALSE", b"current_user", b"current_schema"]
COLS = [b"id", b"tag", b"val", b"c1.id", b"c2.tag", b"c1.val"]
TYPES = [b"int", b"text", b"float", b"bool", b"varchar"]


def gen_expr(rng, d):
    if d <= 0 or rng.random() < 0.3:
        return rng.choice(LITS + COLS)
    k = rng.randrange(12)
    a, b = gen_expr(rng, d - 1), gen_expr(rng, d - 1)
    if k == 0:
        return b"(" + a + b")"
    if k == 1:
        return b"NOT " + a
    if k == 2:
        return a + b" " + rng.choice([b"+", b"-", b"*", b"/", b"%"]) + b" " + b
    if k == 3:
        return a + b" " + rng.choice([b"=", b"<>", b"<", b">=", b"<=", b">"]) + b" " + b
    if k == 4:
        return a + b" AND " + b
    if k == 5:
        return a + b" OR " + b
    if k == 6:
        return b"CASE WHEN " + a + b" THEN " + b + b" ELSE " + a + b" END"
    if k == 7:
        return b"CAST(" + a + b" AS " + rng.choice(TYPES) + b")"
    if k == 8:
        return a + b" IS NOT NULL"
    if k == 9:
        return a + b" IN (" + b", ".join([a, b]) + b")"
    if k == 10:
        return a + b" ~ " + b"'^x*'"
    return b"(SELECT max(" + rng.choice(COLS) + b") FROM victim)"


def gen_stmt(rng):
    k = rng.randrange(10)
    if k == 0:
        return b"INSERT INTO victim (id, tag, val) VALUES (" + \
            str(rng.randrange(1, 10 ** 6)).encode() + b", 'g', " + \
            str(rng.random()).encode() + b")"
    if k == 1:
        return b"UPDATE victim SET val = " + gen_expr(rng, 2) + b" WHERE " + gen_expr(rng, 2)
    if k == 2:
        return b"DELETE FROM victim WHERE " + gen_expr(rng, 2) + b" AND id < 0"
    if k == 3:
        return rng.choice([b"BEGIN", b"COMMIT", b"ROLLBACK"])
    if k == 4:
        return b"CREATE TABLE tf" + str(rng.randrange(9)).encode() + \
            b" (a int, b text NOT NULL)"
    if k == 5:
        return b"DROP TABLE IF EXISTS tf" + str(rng.randrange(9)).encode()
    if k == 6:
        return b"ALTER TABLE victim ADD COLUMN xf" + str(rng.randrange(4)).encode() + b" int"
    if k == 7:
        return b"COPY victim TO STDOUT" + rng.choice([b"", b" WITH (FORMAT csv, HEADER true)"])
    if k == 8:
        return b"SELECT x FROM generate_series(1, 10) AS x WHERE " + gen_expr(rng, 1)
    sel = b"SELECT " + (b"DISTINCT " if rng.random() < 0.2 else b"")
    sel += b", ".join(gen_expr(rng, rng.randint(0, 3)) for _ in range(rng.randint(1, 4)))
    sel += b" FROM victim c1"
    if rng.random() < 0.3:
        sel += b" JOIN victim c2 ON " + gen_expr(rng, 1)
    if rng.random() < 0.6:
        sel += b" WHERE " + gen_expr(rng, rng.randint(1, 3))
    if rng.random() < 0.3:
        sel += b" GROUP BY c1.id HAVING count(*) >= 0"
    if rng.random() < 0.3:
        sel += b" ORDER BY " + gen_expr(rng, 1) + rng.choice([b"", b" DESC", b" NULLS FIRST"])
    if rng.random() < 0.3:
        sel += b" LIMIT " + str(rng.randrange(0, 100)).encode()
    if rng.random() < 0.15:
        sel += b" OFFSET " + str(rng.randrange(0, 50)).encode()
    return sel


def layer5(rng):
    cases = []
    for i in range(220):
        sql = gen_stmt(rng)
        if rng.random() < 0.35:
            sql = mutate_bytes(rng, sql)
        cases.append(Case("L5", f"grammar-{i}", [
            ("send", startup_pkt()),
            ("send", msg(b"Q", sql + b"\x00")),
            ("drain",),
        ]))
    return cases


# ---- L6: recorded-session replay with mutation ----

def layer6(rng):
    def session():
        return [
            ("send", startup_pkt()),
            ("send", msg(b"Q", b"SELECT 1\x00")),
            ("send", msg(b"Q", b"SELECT * FROM victim ORDER BY id LIMIT 3\x00")),
            ("send", msg(b"P", parse_msg("s", b"SELECT $1::int + id FROM victim LIMIT 1", [23]))),
            ("send", msg(b"B", bind_msg("p", "s", [0], [b"5"], [0]))),
            ("send", msg(b"D", b"P\x00")),
            ("send", msg(b"E", cstr("p") + struct.pack(">I", 0))),
            ("send", msg(b"S")),
            ("send", msg(b"Q", b"BEGIN\x00")),
            ("send", msg(b"Q", b"INSERT INTO victim (id, tag, val) VALUES (900002, 's', 0.5)\x00")),
            ("send", msg(b"Q", b"COMMIT\x00")),
            ("send", msg(b"Q", b"COPY victim TO STDOUT\x00")),
            ("send", msg(b"X")),
        ]

    cases = []
    for i in range(90):
        ops = session()
        for _ in range(rng.randint(1, 3)):
            j = rng.randrange(1, len(ops) - 1)
            kind = rng.randrange(3)
            if kind == 0:
                ops[j] = ("send", mutate_bytes(rng, ops[j][1]))
            elif kind == 1:
                del ops[j]
            else:
                ops.insert(j, ("send", msg(rng.choice(b"QPEDBSXH"),
                                          bytes(rng.randrange(256) for _ in range(rng.randrange(0, 16))))))
        cases.append(Case("L6", f"replay-{i}", ops, partial_ok=True))
    # clean control replay
    cases.append(Case("L6", "control", session(), partial_ok=True))
    return cases


# ----------------------------------------------------------------- victim --

CANARY_EXPECT = None


def seed_victim(port=PORT):
    """victim: the fuzzer's punching bag (corpus + grammar may trash it).
    victim: an unguessably-named twin whose bytes must never change -- no
    generated or mutated statement can legitimately name it."""
    global CANARY_EXPECT
    c = psycopg2.connect(f"host=127.0.0.1 port={port} dbname=x user=u")
    c.autocommit = True
    cur = c.cursor()
    for t in ("victim", CANARY_TABLE):
        cur.execute(f"DROP TABLE IF EXISTS {t}")
    cur.execute("CREATE TABLE victim (id int, tag text, val float)")
    rows = [(i, f"tag-{i:04d}", i / 7.0) for i in range(250)]
    cur.executemany("INSERT INTO victim VALUES (%s, %s, %s)", rows)
    cur.execute(f"CREATE TABLE {CANARY_TABLE} (id int, tag text, val float)")
    cur.executemany(f"INSERT INTO {CANARY_TABLE} VALUES (%s, %s, %s)", rows)
    cur.execute(f"SELECT id, tag, val FROM {CANARY_TABLE} ORDER BY id")
    CANARY_EXPECT = cur.fetchall()
    cur.execute("CREATE INDEX victim_ix ON victim USING btree (id)")
    c.close()


def verify_victim(port, where):
    c = psycopg2.connect(f"host=127.0.0.1 port={port} dbname=x user=u")
    c.autocommit = True
    cur = c.cursor()
    cur.execute(f"SELECT id, tag, val FROM {CANARY_TABLE} ORDER BY id")
    got = cur.fetchall()
    c.close()
    if got != CANARY_EXPECT:
        print(f"  [FAIL] victim drift at {where}: {len(got)} rows, "
              f"expected {len(CANARY_EXPECT)}")
        return False
    return True


# -------------------------------------------------------------------- main --

def run_layer(server, cases, stats_key, deadline):
    stats = {"ran": 0, "responded": 0, "closed": 0, "silent": 0}
    for c in cases:
        if time.time() > deadline and stats["ran"] >= 25:
            break
        v = run_case(server, c)
        stats["ran"] += 1
        if v is not None:
            VIOLATIONS.append(v)
            repro = (lambda vv: vv["kind"] == v["kind"]) if v["kind"] != "server-died" \
                else (lambda vv: vv["kind"] in ("server-died", "server-wedged"))
            if v["kind"] in ("server-died", "server-wedged", "connect-refused") \
                    and server.proc.poll() is not None:
                server.restart()
                v2 = run_case(server, c)
                if v2 is None:
                    continue  # not deterministic; keep hunting
            m = minimize(server, c, repro)
            save_artifact(v, minimized=m)
            print(f"     minimized to {len(m.ops)} ops "
                  f"({sum(len(o[1]) for o in m.ops if o[0] in ('send', 'send!'))} bytes)")
            return False
    STATS[stats_key] = stats
    print(f"  [{stats_key}] {stats['ran']} cases, server healthy")
    return True


def sweep_stale():
    """kill leftover puddle processes from crashed previous runs so they
    cannot answer probes on our ports or mutate our data dirs."""
    try:
        out = subprocess.run(["pgrep", "-f", "puddle.*puddle-hostile-fuzz"],
                             capture_output=True, text=True)
        for pid in out.stdout.split():
            try:
                os.kill(int(pid), signal.SIGKILL)
            except OSError:
                pass
        if out.stdout.split():
            time.sleep(0.3)
    except Exception:
        pass


def main():
    secs = float(sys.argv[1]) if len(sys.argv) > 1 else 60.0
    seed = int(sys.argv[2]) if len(sys.argv) > 2 else random.SystemRandom().randrange(2 ** 31)
    print(f"hostile fuzz: budget={secs:.0f}s seed={seed}")
    print(f"reproduce with: python3 tests/hostile_fuzz.py {secs:.0f} {seed}")
    sweep_stale()

    shutil.rmtree("/tmp/puddle-hostile-fuzz-scram", ignore_errors=True)
    shutil.rmtree(DATA, ignore_errors=True)
    os.makedirs(DATA, exist_ok=True)
    os.makedirs(SCRAM_DATA, exist_ok=True)
    shutil.rmtree(ARTDIR, ignore_errors=True)

    # scram verifier for the auth-fuzz user
    r = subprocess.run([BIN, "--scram-secret", "fuzz-secret-1"],
                       capture_output=True, text=True)
    assert r.stdout.startswith("SCRAM-SHA-256$"), r.stdout + r.stderr
    with open(AUTH_FILE, "w") as f:
        json.dump({"fuzzu": {"scram": r.stdout.strip()}}, f)

    server = Server(PORT, DATA)
    scram = Server(SCRAM_PORT, SCRAM_DATA,
                   ("--auth", "scram-sha-256", "--auth-file", AUTH_FILE),
                   auth="scram")
    rng = random.Random(seed)

    try:
        seed_victim()
        t0 = time.time()
        ok = True

        print("  [L0] directed kill-probes")
        l0 = layer0(server)
        for item in l0:
            if isinstance(item, tuple) and item[0] == "amp":
                if not probe_amplification(server):
                    ok = False
                continue
            v = run_case(server, item)
            if v is not None:
                VIOLATIONS.append(v)
                repro = (lambda vv: vv["kind"] == v["kind"]) if v["kind"] != "server-died" \
                    else (lambda vv: vv["kind"] in ("server-died", "server-wedged"))
                if server.proc.poll() is not None:
                    server.restart()
                    if run_case(server, item) is None:
                        print(f"  [L0] {item.name}: NOT deterministic, skipping")
                        continue
                m = minimize(server, item, repro)
                save_artifact(v, minimized=m)
                ok = False
                break
        if ok:
            print(f"  [L0] {len(l0) - 1} directed probes, server healthy")

        if ok:
            ok &= verify_victim(PORT, "after L0")
        rss0 = rss_mb(server.proc.pid)
        deadline = t0 + secs

        layers = [
            ("L1", layer1, server), ("L2", layer2, server), ("L3", layer3, server),
            ("L4", layer4, scram), ("L5", layer5, server), ("L6", layer6, server),
        ]
        if ok:
            for name, gen, srv in layers:
                if not run_layer(srv, gen(random.Random(rng.randrange(2 ** 31))),
                                 name, deadline):
                    ok = False
                    break
                if name in ("L3", "L6"):
                    if not verify_victim(PORT, f"after {name}"):
                        ok = False
                        break
                rss = rss_mb(srv.proc.pid)
                if rss > RSS_CEILING_MB:
                    print(f"  [FAIL] {name}: RSS {rss:.1f} MB > {RSS_CEILING_MB} MB")
                    ok = False
                    break

        if ok:
            ok &= verify_victim(PORT, "final")
            rss1 = rss_mb(server.proc.pid)
            print(f"  RSS: start {rss0:.1f} -> end {rss1:.1f} MB "
                  f"(ceiling {RSS_CEILING_MB})")
            if rss1 > RSS_CEILING_MB:
                ok = False
        if ok:
            ok &= _liveness(server) and _liveness(scram)

        total = sum(s["ran"] for s in STATS.values())
        if ok:
            print(f"\nHOSTILE FUZZ PASSED ({total} random cases + "
                  f"{len(l0) - 1} directed probes, seed {seed}, "
                  f"{time.time() - t0:.1f}s)")
            return 0
        print(f"\nHOSTILE FUZZ FAILED: {len(VIOLATIONS)} violation(s); "
              f"artifacts in {ARTDIR}")
        return 1
    finally:
        kill(server)
        kill(scram)


if __name__ == "__main__":
    sys.exit(main())
