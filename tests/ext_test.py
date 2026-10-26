#!/usr/bin/env python3
"""Raw-socket extended-query-protocol tests for puddle (port in argv)."""
import socket, struct, sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 5438
FAILS = []
N = [0]

def msg(t, body=b""):
    return bytes([ord(t)]) + struct.pack("!I", len(body) + 4) + body

def cstr(s):
    return s.encode() + b"\x00"

class Reader:
    def __init__(self, sock):
        self.s = sock
        self.buf = b""
    def _fill(self):
        self.s.settimeout(5)
        chunk = self.s.recv(65536)
        if not chunk:
            raise ConnectionError("eof")
        self.buf += chunk
    def msg(self):
        while len(self.buf) < 5:
            self._fill()
        t = self.buf[0:1]
        ln = struct.unpack("!I", self.buf[1:5])[0]
        while len(self.buf) < 1 + ln:
            self._fill()
        body = self.buf[5:1 + ln]
        self.buf = self.buf[1 + ln:]
        return t.decode(), body
    def until_ready(self):
        out = []
        while True:
            t, b = self.msg()
            out.append((t, b))
            if t == "Z":
                return out

def check(name, ok, detail=""):
    N[0] += 1
    if not ok:
        FAILS.append(name)
        print(f"  [FAIL] {name} {detail}")
    return ok

s = socket.create_connection(("127.0.0.1", PORT))
params = b"".join(cstr(k) + cstr(v) for k, v in [("user", "u"), ("database", "x")]) + cstr("")
s.sendall(struct.pack("!II", len(params) + 8, 196608) + params)
r = Reader(s)
msgs = r.until_ready()
assert any(t == "R" for t, _ in msgs), f"no auth: {msgs}"

def simple(sql):
    s.sendall(msg("Q", cstr(sql)))
    return r.until_ready()

# setup
simple("DROP TABLE IF EXISTS susp")
simple("CREATE TABLE susp (g int)")
simple("INSERT INTO susp VALUES (1),(2),(3),(4),(5)")

# ---- 1: portal suspension at max_rows=2
s.sendall(msg("P", cstr("") + cstr("SELECT g FROM susp ORDER BY g") + struct.pack("!H", 0))
          + msg("B", cstr("") + cstr("") + struct.pack("!H", 0) + struct.pack("!H", 0)
                + struct.pack("!H", 1) + struct.pack("!H", 1))
          + msg("E", cstr("") + struct.pack("!I", 2))
          + msg("S"))
ms = r.until_ready()
types = [t for t, _ in ms]
datarows = [b for t, b in ms if t == "D"]
check("portal suspended", "s" in types, f"types={types}")
check("2 rows before suspend", len(datarows) == 2, f"{len(datarows)}")

# resume the same portal
s.sendall(msg("E", cstr("") + struct.pack("!I", 0)) + msg("S"))
ms = r.until_ready()
types = [t for t, _ in ms]
datarows = [t for t, b in ms if t == "D"]
check("resume returns rest + complete", datarows == ["D", "D", "D"] and "C" in types, f"types={types}")

# ---- 2: binary int8 result
s.sendall(msg("P", cstr("") + cstr("SELECT 42::int8") + struct.pack("!H", 0))
          + msg("B", cstr("") + cstr("") + struct.pack("!H", 0) + struct.pack("!H", 0)
                + struct.pack("!H", 1) + struct.pack("!H", 1))
          + msg("E", cstr("") + struct.pack("!I", 0)) + msg("S"))
ms = r.until_ready()
ok = False
for t, b in ms:
    if t == "D":
        ncols = struct.unpack("!H", b[:2])[0]
        vlen = struct.unpack("!i", b[2:6])[0]
        val = struct.unpack("!q", b[6:14])[0]
        ok = ncols == 1 and vlen == 8 and val == 42
check("binary int8 BE", ok, f"{[t for t,_ in ms]}")

# ---- 3: Describe(S) -> ParameterDescription + RowDescription
s.sendall(msg("P", cstr("st1") + cstr("SELECT g FROM susp WHERE g > $1") + struct.pack("!H", 0))
          + msg("D", b"S" + cstr("st1")) + msg("S"))
ms = r.until_ready()
types = [t for t, _ in ms]
check("describe stmt", "t" in types and "T" in types, f"types={types}")

# ---- 4: error then Sync recovery
s.sendall(msg("P", cstr("") + cstr("SELECT 1/0") + struct.pack("!H", 0))
          + msg("B", cstr("") + cstr("") + struct.pack("!H", 0) + struct.pack("!H", 0))
          + msg("E", cstr("") + struct.pack("!I", 0)) + msg("S"))
ms = r.until_ready()
check("error reported", any(t == "E" for t, _ in ms), f"{[t for t,_ in ms]}")
ms = simple("SELECT 7")
check("sync recovery", any(t == "D" for t, _ in ms), f"{[t for t,_ in ms]}")

# ---- 5: named prepared statement reuse with params
# text-format parameter (libpq default; binary params are declined with 0A000)
s.sendall(msg("P", cstr("st2") + cstr("SELECT $1::int * 2") + struct.pack("!H", 0))
          + msg("B", cstr("p1") + cstr("st2") + struct.pack("!H", 0) + struct.pack("!H", 1)
                + struct.pack("!i", 2) + b"21"
                + struct.pack("!H", 0))
          + msg("E", cstr("p1") + struct.pack("!I", 0)) + msg("S"))
ms = r.until_ready()
found = None
for t, b in ms:
    if t == "D":
        ncols = struct.unpack("!H", b[:2])[0]
        vlen = struct.unpack("!i", b[2:6])[0]
        found = b[6:6+vlen]
check("bind text param", found == b"42", f"{found!r}")

s.close()
print(f"\n{N[0] - len(FAILS)}/{N[0]} extended-protocol checks passed")
if FAILS:
    print("FAILED:", FAILS)
    sys.exit(1)
print("EXTENDED PROTOCOL TESTS PASSED")
