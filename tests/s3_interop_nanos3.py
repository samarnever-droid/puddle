#!/usr/bin/env python3
"""puddle x nanos3 interop battery.

Runs puddle's S3-mode durability suite against a REAL nanos3 server
(https://github.com/samarnever-droid/nanos3) instead of the in-memory
python mock, plus direct conditional-write probes:

  P1  If-None-Match:* create-once  -> 2nd put must get 412
  P2  If-Match:<etag> CAS          -> wrong etag must get 412
  P3  If-Match on missing object   -> 404 NoSuchKey
  P4  bad SigV4 secret             -> 403 (nanos3 enforces signatures)
then the full s3_test battery (DML, CHECKPOINT, kill -9, dual-writer
loud refusal, fresh-prefix health) with nanos3 as the object store.

Usage: python3 tests/s3_interop_nanos3.py [path-to-nanos3-binary]
"""
import os
import shutil
import signal
import subprocess
import sys
import time

import boto3
import psycopg2
from botocore.config import Config

HERE = os.path.dirname(os.path.abspath(__file__))
NANOS3 = sys.argv[1] if len(sys.argv) > 1 else "/home/user/nanos3/nanos3"
DATA = "/tmp/nanos3-interop-data"
S3PORT = 5512
S3URL = f"http://127.0.0.1:{S3PORT}"
PORT = 5511
PREFIX = "m7"
AK, SK = "testkey", "testsecret"
BUCKET = "puddle-bucket"
FAILS = []


def check(name, got, want):
    ok = got == want
    print(f"  [{'OK' if ok else 'FAIL'}] {name}: {got!r}"
          + ("" if ok else f" != {want!r}"))
    if not ok:
        FAILS.append(name)
    return ok


def client():
    return boto3.client(
        "s3", endpoint_url=S3URL, aws_access_key_id=AK,
        aws_secret_access_key=SK, region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"},
                      signature_version="s3v4", retries={"max_attempts": 0}))


def start_nanos3():
    shutil.rmtree(DATA, ignore_errors=True)
    p = subprocess.Popen(
        [NANOS3, "-data", DATA, "-http", f"127.0.0.1:{S3PORT}", "-https", "off",
         "-access-key", AK, "-secret-key", SK, "-quiet"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    import socket as _s
    for _ in range(200):
        try:
            _s.create_connection(("127.0.0.1", S3PORT), timeout=1).close()
            return p
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("nanos3 did not start")


def start_puddle(port=PORT, prefix=PREFIX):
    p = subprocess.Popen(
        ["/home/user/puddle/target/release/puddle",
         "--data", f"s3://{BUCKET}/{prefix}", "--port", str(port),
         "--bind", "127.0.0.1", "--endpoint", S3URL, "--region", "us-east-1",
         "--access-key", AK, "--secret-key", SK],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(300):
        try:
            psycopg2.connect(f"host=127.0.0.1 port={port} dbname=x user=u "
                             f"sslmode=disable", connect_timeout=1).close()
            return p
        except Exception:
            time.sleep(0.05)
    raise RuntimeError("puddle did not start against nanos3")


def sql(*stmts, port=PORT):
    c = psycopg2.connect(f"host=127.0.0.1 port={port} dbname=x user=u "
                         f"sslmode=disable")
    c.autocommit = True
    cur = c.cursor()
    out = []
    for s in stmts:
        cur.execute(s)
        out.append(cur.fetchall() if cur.description else None)
    c.close()
    return out


def http_status(method, key, headers=None, body=b"", sign=True):
    """Raw signed request; returns (status, etag). Sign=off for the
    bad-secret probe."""
    import hashlib
    import hmac
    import datetime
    import urllib.request
    t = datetime.datetime.now(datetime.timezone.utc)
    amzdate, ds = t.strftime("%Y%m%dT%H%M%SZ"), t.strftime("%Y%m%d")
    host = f"127.0.0.1:{S3PORT}"
    hdrs = {"host": host, "x-amz-date": amzdate}
    if sign:
        hdrs["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
    hdrs.update(headers or {})
    signed = ";".join(sorted(hdrs))
    canon_headers = "".join(f"{k}:{hdrs[k]}\n" for k in sorted(hdrs))
    payload_hash = hdrs.get("x-amz-content-sha256",
                            hashlib.sha256(body).hexdigest())
    creq = "\n".join([method, f"/{BUCKET}/{key}", "", canon_headers,
                      signed, payload_hash])
    scope = f"{ds}/us-east-1/s3/aws4_request"
    sts = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope,
                     hashlib.sha256(creq.encode()).hexdigest()])
    secret = SK if sign else "WRONGSECRET"

    def h(msg, key_):
        return hmac.new(key_, msg.encode(), hashlib.sha256).digest()
    k = h("aws4_request", h("s3", h("us-east-1", h(ds, ("AWS4" + secret).encode()))))
    sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request(
        f"{S3URL}/{BUCKET}/{key}", data=body if method != "HEAD" else None,
        method=method)
    for k_, v_ in hdrs.items():
        if k_ != "host":
            req.add_header(k_, v_)
    req.add_header("Authorization",
                   f"AWS4-HMAC-SHA256 Credential={AK}/{scope}, "
                   f"SignedHeaders={signed}, Signature={sig}")
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, r.headers.get("ETag", "").strip('"')
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("ETag", "").strip('"')


def main():
    s3p = start_nanos3()
    try:
        s3 = client()
        s3.create_bucket(Bucket=BUCKET)
        print(f"nanos3 up at {S3URL} (bucket {BUCKET} created, SigV4 ok)")

        # ---- conditional-write probes (the commit protocol's foundation)
        s3.put_object(Bucket=BUCKET, Key="probe/one", Body=b"v1")
        etag = s3.head_object(Bucket=BUCKET, Key="probe/one")["ETag"].strip('"')
        check("P0 plain put/get works",
              s3.get_object(Bucket=BUCKET, Key="probe/one")["Body"].read(), b"v1")
        st, _ = http_status("PUT", "probe/one", {"if-none-match": "*"}, b"v2")
        check("P1 If-None-Match:* create-once rejected (412)", st, 412)
        st, _ = http_status("PUT", "probe/none", {"if-none-match": "*"}, b"new")
        check("P1b If-None-Match:* on absent key succeeds (200)", st, 200)
        st, _ = http_status("PUT", "probe/one", {"if-match": '"deadbeef"'}, b"v3")
        check("P2 If-Match wrong etag rejected (412)", st, 412)
        st, _ = http_status("PUT", "probe/one", {"if-match": f'"{etag}"'}, b"v3")
        check("P2b If-Match correct etag succeeds (200)", st, 200)
        st, _ = http_status("PUT", "probe/missing", {"if-match": '"x"'}, b"v")
        check("P3 If-Match on missing key -> 404", st, 404)
        st, _ = http_status("PUT", "probe/bad", {}, b"x", sign=False)
        check("P4 bad SigV4 secret -> 403", st, 403)

        if FAILS:
            print("\nconditional-write probes FAILED; the puddle commit "
                  "protocol (manifest create-once, meta.json CAS) cannot be "
                  "safe against this server.")
            print("SKIPPING the durability battery (it would prove nothing).")
            return

        # ---- full durability battery against nanos3
        print("\npuddle S3-mode battery (nanos3 as the object store):")
        p = start_puddle()
        sql("CREATE TABLE t (id int, v text)",
            "INSERT INTO t VALUES " + ",".join(f"({i},'s3-{i}')" for i in range(200)),
            "CREATE TABLE u (x float)",
            "INSERT INTO u VALUES (1.5), (2.5)")
        sql("CHECKPOINT")
        sql("INSERT INTO t VALUES "
            + ",".join(f"({200+i},'post-{i}')" for i in range(50)))
        check("rows before crash", sql("SELECT count(*) FROM t")[0], [(250,)])

        os.kill(p.pid, signal.SIGKILL); p.wait()
        p = start_puddle()
        check("rows after kill -9 + restart",
              sql("SELECT count(*) FROM t")[0], [(250,)])
        check("post-checkpoint row recovered",
              sql("SELECT v FROM t WHERE id = 217")[0], [("post-17",)])
        check("second table recovered",
              sql("SELECT sum(x) FROM u")[0], [(4.0,)])
        check("LIKE filter on recovered data",
              sql("SELECT count(*) FROM t WHERE v LIKE 'post-%'")[0], [(50,)])

        # snapshots over nanos3 (v2.0)
        sql("CREATE SNAPSHOT interop_snap")
        sql("DELETE FROM t WHERE id < 50")
        check("asof sees pre-delete state",
              sql("SELECT count(*) FROM t AS OF SNAPSHOT interop_snap")[0],
              [(250,)])
        check("live sees deletes",
              sql("SELECT count(*) FROM t")[0], [(200,)])

        # ---- dual writer: recovery must refuse LOUDLY, never silently
        P2 = PORT + 10
        p2 = start_puddle(port=P2)
        check("second process sees same bucket state",
              sql("SELECT count(*) FROM t", port=P2)[0], [(200,)])
        try:
            sql("INSERT INTO t VALUES (99999, 'from-p2')", port=P2)
            check("p2 sees its own write",
                  sql("SELECT count(*) FROM t", port=P2)[0], [(201,)])
        except Exception as e:
            print(f"  [info] p2 write rejected (acceptable): {str(e)[:60]}")
        sql("INSERT INTO t VALUES (88888, 'from-p1')")
        os.kill(p2.pid, signal.SIGKILL); p2.wait()
        os.kill(p.pid, signal.SIGKILL); p.wait()
        try:
            p = start_puddle()
            print("  [FAIL] dual-writer recovery started silently")
            FAILS.append("dual-writer recovery refused")
            os.kill(p.pid, signal.SIGKILL); p.wait()
        except RuntimeError:
            print("  [OK] dual-writer state refused loudly at recovery")
        # binary healthy on a fresh prefix
        p = start_puddle(prefix="m7-fresh")
        sql("CREATE TABLE fresh (v int)",
            "INSERT INTO fresh VALUES " + ",".join(f"({i})" for i in range(20)))
        check("fresh prefix healthy after refusal",
              sql("SELECT count(*), sum(v) FROM fresh")[0], [(20, 190)])
        os.kill(p.pid, signal.SIGKILL); p.wait()
    finally:
        try:
            s3p.send_signal(signal.SIGTERM); s3p.wait(timeout=5)
        except Exception:
            s3p.kill()

    print()
    if FAILS:
        print(f"NANOS3 INTEROP FAILED: {FAILS}")
        sys.exit(1)
    print("NANOS3 INTEROP PASSED (conditional writes + full S3 battery)")


if __name__ == "__main__":
    main()
