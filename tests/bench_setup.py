#!/usr/bin/env python3
"""Build the benchmark tables. Usage: bench_setup.py PORT ROWS"""
import sys, time
import psycopg2

port, rows = int(sys.argv[1]), int(sys.argv[2])
c = psycopg2.connect(f"host=127.0.0.1 port={port} dbname=x user=u")
c.autocommit = True
k = c.cursor()
k.execute("DROP TABLE IF EXISTS bench_big")
k.execute("DROP TABLE IF EXISTS bench_dim")
k.execute("CREATE TABLE bench_big (a int, x float, g int, label text)")
t0 = time.perf_counter()
CH = 5000
for base in range(0, rows, CH):
    vals = ",".join(
        k.mogrify("(%s,%s,%s,%s)", (i, i * 0.5, i % 100, f"row-{i % 997}")).decode()
        for i in range(base, min(base + CH, rows))
    )
    k.execute(f"INSERT INTO bench_big VALUES {vals}")
load = time.perf_counter() - t0
k.execute("CREATE TABLE bench_dim (g int, gname text)")
for g in range(100):
    k.execute("INSERT INTO bench_dim VALUES (%s, %s)", (g, f"group-{g}"))
k.execute("SELECT count(*) FROM bench_big")
n = k.fetchall()[0][0]
assert n == rows, (n, rows)
print(f"LOADED {rows} rows in {load:.2f}s")
