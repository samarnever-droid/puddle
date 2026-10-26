#!/usr/bin/env python3
"""Run one query best-of-n. Usage: bench_worker.py PORT SQL N  -> prints ms + result."""
import sys, time
import psycopg2

port, sql, n = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
c = psycopg2.connect(f"host=127.0.0.1 port={port} dbname=x user=u")
c.autocommit = True
k = c.cursor()
k.execute(sql)
first = k.fetchall()
times = []
for _ in range(n):
    t0 = time.perf_counter()
    k.execute(sql)
    k.fetchall()
    times.append(time.perf_counter() - t0)
print(min(times) * 1000)
print(repr(first)[:200])
