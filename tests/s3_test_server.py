#!/usr/bin/env python3
"""S3-compatible test server that INDEPENDENTLY verifies AWS SigV4 server-side.

Every request's Authorization header is recomputed from the raw request
(host, method, path, query, signed headers, payload hash) and must match
exactly — so puddle's hand-rolled signer is validated for real.

Implements: PUT (+If-None-Match:*, +If-Match), GET (+Range), HEAD, DELETE,
LIST (list-type=2, prefix, max-keys paging, continuation-token).
Objects are stored in memory; etag = sha256 hex of contents.

Usage: python3 s3_test_server.py [port]
Env:   S3TEST_ACCESS_KEY (default testkey), S3TEST_SECRET_KEY (default testsecret)
"""
import hashlib, hmac, json, sys, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qsl, unquote

BUCKET = "puddle-bucket"
STORE = {}          # key -> bytes
LOCK = threading.Lock()
ACCESS = __import__("os").environ.get("S3TEST_ACCESS_KEY", "testkey")
SECRET = __import__("os").environ.get("S3TEST_SECRET_KEY", "testsecret")
REGION = "us-east-1"
STATS = {"requests": 0, "sig_verified": 0, "cond_412": 0}
PAGE = 3            # max-keys per LIST page (forces continuation tokens)


def sha256_hex(b):
    return hashlib.sha256(b).hexdigest()


def derive_key(secret, date, region, service):
    k = hmac.new(("AWS4" + secret).encode(), date.encode(), hashlib.sha256).digest()
    k = hmac.new(k, region.encode(), hashlib.sha256).digest()
    k = hmac.new(k, service.encode(), hashlib.sha256).digest()
    return hmac.new(k, b"aws4_request", hashlib.sha256).digest()


def uri_encode(s, slash=False):
    out = []
    for b in s.encode():
        c = chr(b)
        if c.isascii() and (c.isalnum() or c in "-._~"):
            out.append(c)
        elif c == "/" and not slash:
            out.append(c)
        else:
            out.append("%%%02X" % b)
    return "".join(out)


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "S3Test/1.0"

    def log_message(self, fmt, *args):
        sys.stderr.write("  s3: %s\n" % (fmt % args))

    # ------------------------------------------------------------ helpers --

    def verify_sig(self, payload_hash_expected=None):
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("AWS4-HMAC-SHA256 "):
            return False, "no sigv4 auth header"
        parts = dict(p.strip().split("=", 1) for p in auth[len("AWS4-HMAC-SHA256 "):].split(", "))
        cred = parts["Credential"].split("/")
        ak, date, region, service, term = cred[0], cred[1], cred[2], cred[3], cred[4]
        if ak != ACCESS or region != REGION or service != "s3" or term != "aws4_request":
            return False, "bad credential scope"
        stamp = parts["X-Amz-Date"] if "X-Amz-Date" in parts else self.headers.get("x-amz-date", "")
        # canonical request from the ACTUAL request
        u = urlparse(self.path)
        path = u.path
        query_pairs = sorted(parse_qsl(u.query, keep_blank_values=True))
        canon_q = "&".join(f"{uri_encode(k, True)}={uri_encode(v, True)}" for k, v in query_pairs)
        signed = parts["SignedHeaders"].split(";")
        canon_h = "".join(f"{h}:{self.headers.get(h, '').strip()}\n" for h in signed)
        payload_hash = self.headers.get("x-amz-content-sha256", "")
        if payload_hash_expected and payload_hash != payload_hash_expected:
            return False, "payload hash mismatch"
        cr = "\n".join([self.command, path, canon_q, canon_h, parts["SignedHeaders"], payload_hash])
        scope = f"{date}/{region}/{service}/aws4_request"
        sts = "\n".join(["AWS4-HMAC-SHA256", stamp, scope, hashlib.sha256(cr.encode()).hexdigest()])
        sig = hmac.new(derive_key(SECRET, date, region, service), sts.encode(), hashlib.sha256).hexdigest()
        if sig != parts["Signature"]:
            return False, f"signature mismatch: got {parts['Signature'][:16]}.. want {sig[:16]}.. (cr={cr!r})"
        return True, None

    def send(self, code, body=b"", etag=None, extra=None):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        if etag:
            self.send_header("ETag", f'"{etag}"')
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def read_body(self):
        n = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(n) if n else b""

    def obj_key(self):
        u = urlparse(self.path)
        p = unquote(u.path)
        prefix = f"/{BUCKET}/"
        if not p.startswith(prefix):
            return None
        return p[len(prefix):]

    # -------------------------------------------------------------- verbs --

    def do_PUT(self):
        body = self.read_body()
        key = self.obj_key()
        if key is None or not key:
            return self.send(400, b"bad key")
        if self.headers.get("x-amz-content-sha256") != sha256_hex(body):
            return self.send(400, b"x-amz-content-sha256 does not match body")
        ok, err = self.verify_sig(sha256_hex(body))
        STATS["requests"] += 1
        if not ok:
            return self.send(403, f"SignatureDoesNotMatch: {err}".encode())
        STATS["sig_verified"] += 1
        inm = self.headers.get("If-None-Match")
        im = self.headers.get("If-Match")
        with LOCK:
            exists = key in STORE
            if inm == "*" and exists:
                STATS["cond_412"] += 1
                return self.send(412, b"PreconditionFailed: object exists")
            if im is not None:
                want = im.strip('"')
                cur = sha256_hex(STORE[key]) if exists else None
                if cur != want:
                    STATS["cond_412"] += 1
                    return self.send(412, b"PreconditionFailed: etag mismatch")
            STORE[key] = body
            etag = sha256_hex(body)
        return self.send(200, b"", etag=etag)

    def do_GET(self):
        if self.path == "/__stats":
            body = json.dumps(STATS).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        u = urlparse(self.path)
        key = self.obj_key()
        if (key == "" or key is None) and "list-type" in u.query:
            return self.do_LIST()
        if key is None or key == "":
            return self.send(400, b"bad key")
        STATS["requests"] += 1
        ok, err = self.verify_sig()
        if not ok:
            return self.send(403, f"SignatureDoesNotMatch: {err}".encode())
        STATS["sig_verified"] += 1
        with LOCK:
            if key not in STORE:
                return self.send(404, b"NoSuchKey")
            data = STORE[key]
        rng = self.headers.get("Range")
        if rng and rng.startswith("bytes="):
            a, b = rng[len("bytes="):].split("-")
            a = int(a)
            b = min(int(b), len(data) - 1)
            chunk = data[a:b + 1]
            return self.send(206, chunk, etag=sha256_hex(data),
                             extra={"Content-Range": f"bytes {a}-{b}/{len(data)}"})
        return self.send(200, data, etag=sha256_hex(data))

    def do_HEAD(self):
        key = self.obj_key()
        STATS["requests"] += 1
        ok, err = self.verify_sig()
        if not ok:
            # HEAD can't carry a body; still count it
            self.send_response(403)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        STATS["sig_verified"] += 1
        with LOCK:
            if key not in STORE:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            data = STORE[key]
        return self.send(200, b"", etag=sha256_hex(data),
                         extra={"Content-Length": str(len(data))}) if False else self._head_ok(data)

    def _head_ok(self, data):
        self.send_response(200)
        self.send_header("ETag", f'"{sha256_hex(data)}"')
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()

    def do_DELETE(self):
        key = self.obj_key()
        STATS["requests"] += 1
        ok, err = self.verify_sig()
        if not ok:
            return self.send(403, f"SignatureDoesNotMatch: {err}".encode())
        STATS["sig_verified"] += 1
        with LOCK:
            STORE.pop(key, None)
        return self.send(204, b"")

    def do_LIST(self):
        u = urlparse(self.path)
        q = dict(parse_qsl(u.query, keep_blank_values=True))
        STATS["requests"] += 1
        ok, err = self.verify_sig()
        if not ok:
            return self.send(403, f"SignatureDoesNotMatch: {err}".encode())
        STATS["sig_verified"] += 1
        prefix = q.get("prefix", "")
        token = q.get("continuation-token")
        with LOCK:
            keys = sorted(k for k in STORE if k.startswith(prefix))
        # deterministic paging at PAGE keys per response; token = next index
        start = int(token) if token and token.isdigit() else 0
        page = keys[start:start + PAGE]
        more = start + PAGE < len(keys)
        next_token = str(start + PAGE) if more else None
        def esc(s):
            return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
        xml = ['<?xml version="1.0" encoding="UTF-8"?>', "<ListBucketResult>"]
        for k in page:
            xml.append(f"<Key>{esc(k)}</Key>")
        xml.append(f"<IsTruncated>{'true' if more else 'false'}</IsTruncated>")
        if more:
            xml.append(f"<NextContinuationToken>{next_token}</NextContinuationToken>")
        xml.append("</ListBucketResult>")
        return self.send(200, "".join(xml).encode())


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 4569
    srv = ThreadingHTTPServer(("127.0.0.1", port), H)
    print(f"* s3 test server on 127.0.0.1:{port} bucket={BUCKET} ak={ACCESS}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print("STATS:", json.dumps(STATS), file=sys.stderr)


if __name__ == "__main__":
    main()
