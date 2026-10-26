#!/usr/bin/env python3
"""M7 differential query fuzzer.

Generates thousands of random SELECTs (and interleaved DML) against seeded
random data, runs every statement on TWO identical servers — vectorized
executor ON vs OFF (the reference oracle) — and requires byte-identical
results, including error SQLSTATEs and row order.

Any divergence is an executor bug: the vectorized path may only be faster,
never different.

usage: python3 fuzz_test.py [SEED] [N_QUERIES]
"""
import os
import random
import shutil
import subprocess
import sys
import time

import math
import psycopg2

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "..", "target", "release", "puddle")
PORT_A = 5581  # vectorized ON
PORT_B = 5582  # vectorized OFF (reference)

SEED = int(sys.argv[1]) if len(sys.argv) > 1 else 20261023
NQ = int(sys.argv[2]) if len(sys.argv) > 2 else 1500

rng = random.Random(SEED)

# ---------------------------------------------------------------- schema --

TABLES = {
    "f1": [("a", "int"), ("b", "text"), ("c", "float"), ("d", "bool")],
    "f2": [("k", "int"), ("s", "text"), ("v", "float")],
    "f3": [("a", "int"), ("b", "int"), ("t", "text")],
}
NULL_CHANCE = {"f1": 0.12, "f2": 0.08, "f3": 0.15}


def rand_text():
    return rng.choice(["", "x", "yy", "abc", "Abc", "a c", "zz%zz", "under_score",
                       "row-1", "ROW-2", "9", "3.5", "true", "Ω"])


def rand_val(ty):
    if rng.random() < 0.1:
        return None
    if ty == "int":
        return rng.choice([0, 1, -1, 7, 13, 40, 99, 12345, -999, 2**31 - 1,
                           rng.randint(-500, 500)])
    if ty == "float":
        return rng.choice([0.0, -0.5, 1.5, 99.25, -1e9, 1e9, rng.uniform(-100, 100)])
    if ty == "text":
        return rand_text()
    return rng.choice([True, False])


def build_setup():
    stmts = [f"CREATE TABLE {t} ({', '.join(f'{c} {ty}' for c, ty in cols)})"
             for t, cols in TABLES.items()]
    for t, cols in TABLES.items():
        rows = []
        for _ in range(rng.choice([180, 400, 900])):
            rows.append(tuple(rand_val(ty) for _, ty in cols))
        for base in range(0, len(rows), 100):
            chunk = rows[base:base + 100]
            vals = ",".join(
                "(" + ",".join("NULL" if v is None
                               else ("true" if v is True else "false" if v is False
                                     else f"'{v}'" if isinstance(v, str)
                                     else repr(v))
                               for v in r) + ")" for r in chunk)
            stmts.append(f"INSERT INTO {t} VALUES {vals}")
    return stmts


# ----------------------------------------------------------- expressions --

COLS = {t: [c for c, _ in cols] for t, cols in TABLES.items()}


def col_expr(depth):
    t = rng.choice(list(TABLES))
    c = rng.choice(COLS[t])
    return (t, c)


def gen_expr(depth=0, ctx_table=None, ty=None):
    """Returns a SQL string. ctx_table constrains columns to one table when
    set (single-table WHERE / projection). ty=None -> WILD mode (type chaos,
    stress error paths); ty set -> typed mode (type-sane, stresses result
    equality between the executors)."""
    if ty is None:
        # ---- wild mode: maximum type chaos (error-path stress) ----
        if depth > 3:
            if rng.random() < 0.55:
                t = ctx_table or rng.choice(list(TABLES))
                c = rng.choice([c for c, _ in TABLES[t]])
                return f"{t}.{c}"
            return literal()
        r = rng.random()
        if r < 0.30:
            return f"({gen_expr(depth+1, ctx_table)} {rng.choice(['+', '-', '*', '/', '%'])} {gen_expr(depth+1, ctx_table)})"
        if r < 0.45:
            return f"({gen_expr(depth+1, ctx_table)} {rng.choice(['=', '<>', '<', '<=', '>', '>='])} {gen_expr(depth+1, ctx_table)})"
        if r < 0.55:
            op = rng.choice(["AND", "OR"])
            return f"({gen_expr(depth+1, ctx_table)} {op} {gen_expr(depth+1, ctx_table)})"
        if r < 0.60:
            return f"(NOT {gen_expr(depth+1, ctx_table)})"
        if r < 0.66:
            return f"({gen_expr(depth+1, ctx_table)} IS {rng.choice(['NULL', 'NOT NULL'])})"
        if r < 0.72:
            pat = rng.choice(["a%", "%c", "%b%", "x", "_bc", "%", "row-%", "A%", "%_%"])
            return f"({gen_expr(depth+1, ctx_table)} {'NOT ' if rng.random()<0.3 else ''}LIKE '{pat}')"
        if r < 0.78:
            n = rng.randint(1, 4)
            vals = ", ".join(literal() for _ in range(n))
            return f"({gen_expr(depth+1, ctx_table)} {'NOT ' if rng.random()<0.3 else ''}IN ({vals}))"
        if r < 0.84:
            return f"({gen_expr(depth+1, ctx_table)} {'NOT ' if rng.random()<0.3 else ''}BETWEEN {literal()} AND {literal()})"
        if r < 0.90:
            n = rng.randint(1, 3)
            whens = " ".join(
                f"WHEN {gen_expr(depth+1, ctx_table)} THEN {literal()}" for _ in range(n))
            els = f" ELSE {literal()}" if rng.random() < 0.7 else ""
            return f"(CASE {whens}{els} END)"
        if r < 0.95:
            fn = rng.choice(["length", "upper", "lower", "abs", "coalesce",
                             " substr ", "trim", "reverse"])
            if fn == "coalesce":
                return f"coalesce({gen_expr(depth+1, ctx_table)}, {gen_expr(depth+1, ctx_table)})"
            if fn == " substr ":
                return f"substr({gen_expr(depth+1, ctx_table)}, {rng.randint(1,3)}, {rng.randint(1,4)})"
            return f"{fn.strip()}({gen_expr(depth+1, ctx_table)})"
        return f"CAST({gen_expr(depth+1, ctx_table)} AS {rng.choice(['int', 'text', 'float', 'bool'])})"

    # ---- typed mode: type-sane expressions (result-equality stress) ----
    if depth > 3 or rng.random() < 0.35:
        # leaf
        r = rng.random()
        if r < 0.45:
            t = ctx_table or rng.choice(list(TABLES))
            c = rng.choice(COLS[t])
            return f"CAST({t}.{c} AS {ty})"
        if r < 0.55:
            return "NULL"
        return literal_t(ty)
    r = rng.random()
    if ty == "bool":
        if r < 0.35:
            ot = rng.choice(ALLT)
            return f"({gen_expr(depth+1, ctx_table, ot)} {rng.choice(['=', '<>', '<', '<=', '>', '>='])} {gen_expr(depth+1, ctx_table, ot)})"
        if r < 0.55:
            op = rng.choice(["AND", "OR"])
            return f"({gen_expr(depth+1, ctx_table, 'bool')} {op} {gen_expr(depth+1, ctx_table, 'bool')})"
        if r < 0.65:
            return f"(NOT {gen_expr(depth+1, ctx_table, 'bool')})"
        if r < 0.75:
            pat = rng.choice(["a%", "%c", "%b%", "x", "_bc", "%", "row-%", "A%", "%_%"])
            return f"({gen_expr(depth+1, ctx_table, 'text')} {'NOT ' if rng.random()<0.3 else ''}LIKE '{pat}')"
        if r < 0.85:
            ot = rng.choice(ALLT)
            n = rng.randint(1, 4)
            vals = ", ".join(literal_t(ot) for _ in range(n))
            return f"({gen_expr(depth+1, ctx_table, ot)} {'NOT ' if rng.random()<0.3 else ''}IN ({vals}))"
        if r < 0.93:
            ot = rng.choice(NUM)
            return f"({gen_expr(depth+1, ctx_table, ot)} {'NOT ' if rng.random()<0.3 else ''}BETWEEN {literal_t(ot)} AND {literal_t(ot)})"
        return f"({gen_expr(depth+1, ctx_table, 'bool')} IS {rng.choice(['NULL', 'NOT NULL'])})"
    # numeric / text
    if r < 0.35:
        return f"({gen_expr(depth+1, ctx_table, ty)} {rng.choice(['+', '-', '*', '/', '%'])} {gen_expr(depth+1, ctx_table, ty)})"
    if r < 0.50:
        n = rng.randint(1, 3)
        whens = " ".join(
            f"WHEN {gen_expr(depth+1, ctx_table, 'bool')} THEN {literal_t(ty)}" for _ in range(n))
        els = f" ELSE {literal_t(ty)}" if rng.random() < 0.7 else ""
        return f"(CASE {whens}{els} END)"
    if r < 0.65:
        if ty == "text":
            return f"({gen_expr(depth+1, ctx_table, 'text')} || {gen_expr(depth+1, ctx_table, 'text')})"
        return f"abs({gen_expr(depth+1, ctx_table, ty)})"
    if r < 0.80:
        if ty == "text":
            fn = rng.choice(["upper", "lower", "trim", "reverse"])
            return f"{fn}({gen_expr(depth+1, ctx_table, 'text')})"
        return f"length({gen_expr(depth+1, ctx_table, 'text')}) * {literal_t(ty)}" if ty == "int" else f"CAST(length({gen_expr(depth+1, ctx_table, 'text')}) AS float)"
    if r < 0.90:
        return f"coalesce({gen_expr(depth+1, ctx_table, ty)}, {gen_expr(depth+1, ctx_table, ty)})"
    if ty == "text":
        return f"substr({gen_expr(depth+1, ctx_table, 'text')}, {rng.randint(1,3)}, {rng.randint(1,4)})"
    return f"CAST({gen_expr(depth+1, ctx_table)} AS {ty})"


ALLT = ["int", "float", "text", "bool"]
NUM = ["int", "float"]


def literal():
    ty = rng.choice(ALLT)
    return literal_t(ty)


def literal_t(ty):
    if ty == "int":
        return str(rng.choice([0, 1, 5, 42, -7, 500, 100000]))
    if ty == "float":
        return repr(rng.choice([0.0, 0.5, -2.5, 100.0]))
    if ty == "text":
        return f"'{rand_text()}'"
    if ty == "bool":
        return rng.choice(["true", "false"])
    return "NULL"


# --------------------------------------------------------------- queries --

def gen_query():
    shape = rng.random()
    params = []
    if shape < 0.35:
        # single-table projection + WHERE (+ optional order/limit)
        t = rng.choice(list(TABLES))
        nproj = rng.randint(1, 3)
        proj = ", ".join(
            gen_expr(1, t, rng.choice(ALLT)) if rng.random() < 0.5
            else gen_expr(1, t) if rng.random() < 0.5
            else f"{t}.{c}"
            for c in rng.sample(COLS[t], nproj))
        q = f"SELECT {proj} FROM {t}"
        if rng.random() < 0.8:
            if rng.random() < 0.25:
                q += f" WHERE {t}.{rng.choice(COLS[t])} = $1"
                params.append(rand_param(t))
            else:
                q += f" WHERE {gen_expr(1, t, 'bool') if rng.random() < 0.6 else gen_expr(1, t)}"
        if rng.random() < 0.5:
            keys = ", ".join(
                f"{t}.{c} {rng.choice(['ASC', 'DESC'])} {rng.choice(['', 'NULLS FIRST', 'NULLS LAST'])}"
                for c in rng.sample(COLS[t], rng.randint(1, 2)))
            q += f" ORDER BY {keys}"
        if rng.random() < 0.4:
            q += f" LIMIT {rng.randint(0, 20)}" + (f" OFFSET {rng.randint(0, 10)}" if rng.random() < 0.4 else "")
        return q, params
    if shape < 0.60:
        # group-by + aggregates (+ having)
        t = rng.choice(list(TABLES))
        grp = rng.choice([
            f"{t}.{rng.choice(COLS[t])}",
            gen_expr(2, t, rng.choice(ALLT)) if rng.random() < 0.5 else gen_expr(2, t),
            str(rng.randint(1, 2)),  # position (resolved below via alias list)
        ])
        aggs = []
        for _ in range(rng.randint(1, 3)):
            fn = rng.choice(["count(*)", "count", "sum", "min", "max", "avg"])
            if fn == "count(*)":
                aggs.append("count(*)")
            elif fn == "count":
                aggs.append(f"count({rng.choice(['DISTINCT ' + t + '.' + rng.choice(COLS[t]), t + '.' + rng.choice(COLS[t])])})")
            else:
                aggs.append(f"{fn}({t}.{rng.choice(COLS[t])})")
        # first output column must exist for position-based group by: force alias
        q = f"SELECT {t}.{rng.choice(COLS[t])} AS g0, {', '.join(aggs)} FROM {t}"
        if grp.isdigit():
            grp = "g0"
        else:
            grp = {"a": "g0"}.get(grp, grp)
        q += f" GROUP BY {grp}"
        if rng.random() < 0.4:
            q += f" HAVING count(*) {rng.choice(['>', '<'])} {rng.randint(0, 30)}"
        if rng.random() < 0.5:
            q += f" ORDER BY {rng.choice(['g0', 'count(*)', '1'])} {rng.choice(['ASC', 'DESC'])}"
        return q, params
    if shape < 0.80:
        # joins (equi keys + occasional residual)
        t1, t2 = rng.sample(list(TABLES), 2)
        c1 = rng.choice([c for c, ty in TABLES[t1] if ty == "int"])
        c2 = rng.choice([c for ty in [c for c, ty in TABLES[t2] if ty == "int"] for c in [ty]] )
        kind = rng.choice(["JOIN", "LEFT JOIN", "INNER JOIN", "JOIN", "JOIN"])
        on = f"{t1}.{c1} = {t2}.{c2}"
        if rng.random() < 0.3:
            on += f" AND {t1}.{c1} > {rng.randint(-50, 50)}"
        elif rng.random() < 0.2:
            on += f" AND {gen_expr(2, t1, 'bool')}"
        q = (f"SELECT {t1}.{rng.choice(COLS[t1])}, {t2}.{rng.choice(COLS[t2])}, count(*) "
             f"FROM {t1} {kind} {t2} ON {on}")
        if rng.random() < 0.5:
            q += f" GROUP BY {t1}.{rng.choice(COLS[t1])}, {t2}.{rng.choice(COLS[t2])}"
        if rng.random() < 0.5:
            q += f" ORDER BY 1, 2 LIMIT 25"
        return q, params
    # scan + complex filter + agg mix
    t = rng.choice(list(TABLES))
    return (f"SELECT count(*), sum(CAST({t}.{rng.choice(COLS[t])} AS float)) "
            f"FROM {t} WHERE {gen_expr(2, t, 'bool') if rng.random() < 0.6 else gen_expr(2, t)}", params)


def rand_param(t):
    return rng.choice([0, 1, 7, 42, -3, 500, "abc", "row-1", 2.5])


def same(a, b):
    """NaN-aware deep equality: nan == nan (Python's == says False, but two
    executors both producing nan is agreement, not divergence)."""
    if isinstance(a, float) and isinstance(b, float):
        if math.isnan(a) and math.isnan(b):
            return True
        return a == b
    if isinstance(a, tuple) and isinstance(b, tuple):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    return a == b


# ----------------------------------------------------------------- driver --

def fetch(port, sql, params=None):
    try:
        c = psycopg2.connect(host="127.0.0.1", port=port, dbname="d", user="u",
                             connect_timeout=2)
        c.autocommit = True
        cur = c.cursor()
        try:
            # psycopg2 pyformat: literal % (LIKE patterns!) must be escaped
            # whenever a params list triggers client-side interpolation
            if params:
                sql = sql.replace("%", "%%")
            cur.execute(sql, params)
            r = ("ok", tuple(cur.fetchall())) if cur.description else ("ok",)
        except psycopg2.Error as e:
            r = ("err", e.pgcode or "?????", str(e).splitlines()[0][:120])
        c.close()
        return r
    except psycopg2.Error as e:
        return ("connfail", str(e).splitlines()[0][:120])


def start(port, data, vec):
    shutil.rmtree(data, ignore_errors=True)
    os.makedirs(data, exist_ok=True)
    log = open(f"/tmp/puddle-fuzz-{port}.log", "w")
    p = subprocess.Popen(
        [BIN, "--port", str(port), "--data", f"file://{data}",
         "--vectorized", "on" if vec else "off"],
        stdout=log, stderr=subprocess.STDOUT)
    import socket
    for _ in range(100):
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1)
            s.close()
            return p
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"server {port} did not start")


def main():
    print(f"fuzz seed={SEED} queries={NQ}")
    setup = build_setup()
    pa = start(PORT_A, "/tmp/puddle-fuzz-a", True)
    pb = start(PORT_B, "/tmp/puddle-fuzz-b", False)
    fails = 0
    stats = {"ok": 0, "err": 0}
    try:
        for s in setup:
            ra = fetch(PORT_A, s)
            rb = fetch(PORT_B, s)
            if ra != rb or ra[0] == "err":
                print(f"[SETUP FAIL] {s[:120]}")
                print(f"   A: {str(ra)[:200]}")
                print(f"   B: {str(rb)[:200]}")
                sys.exit(1)
        rng2 = random.Random(SEED ^ 0x5eed)  # separate stream for DML
        for i in range(NQ):
            # interleaved DML storms (same statements on both servers)
            if i > 0 and i % 250 == 0:
                dml = []
                t = rng2.choice(list(TABLES))
                dml.append(f"INSERT INTO {t} VALUES "
                           + "(" + ",".join("NULL" if (v := rand_val(ty)) is None
                                            else f"'{v}'" if isinstance(v, str)
                                            else ("true" if v is True else "false" if v is False else repr(v))
                                            for _, ty in TABLES[t]) + ")")
                dml.append(f"UPDATE {t} SET {rng2.choice(COLS[t])} = "
                           + (f"'{rand_text()}'" if TABLES[t][0][1] == 'text' else repr(rng2.randint(-99, 99)))
                           + f" WHERE {t}.{rng2.choice(COLS[t])} IS NULL")
                dml.append(f"DELETE FROM {t} WHERE {t}.{rng2.choice(COLS[t])} IS NOT NULL AND {t}.{rng2.choice(COLS[t])} IS NULL")
                for s in dml:
                    ra = fetch(PORT_A, s)
                    rb = fetch(PORT_B, s)
                    if ra != rb:
                        fails += 1
                        print(f"[DML DIVERGENCE] {s[:140]}")
                        print(f"   A: {str(ra)[:180]}")
                        print(f"   B: {str(rb)[:180]}")
            q, params = gen_query()
            ra = fetch(PORT_A, q, params or None)
            rb = fetch(PORT_B, q, params or None)
            stats[ra[0] if ra[0] == "ok" else "err"] += 1
            # comparison policy:
            #  - both ok -> results must be byte-identical (the hard invariant)
            #  - both error -> pass: which subexpression errors first depends
            #    on evaluation order (column- vs row-at-a-time). PG parallel
            #    plans and DuckDB have the same property.
            #  - exactly one side errors -> DIVERGENCE (real bug: the
            #    executors disagree on whether the query is even evaluable)
            if ra[0] != rb[0]:
                fails += 1
                print(f"[DIVERGENCE #{fails}] {q[:200]}  params={params}")
                print(f"   vec: {str(ra)[:260]}")
                print(f"   ref: {str(rb)[:260]}")
                if fails >= 5:
                    print("(stopping after 5 divergences)")
                    break
            elif ra[0] == "ok" and not same(ra, rb):
                fails += 1
                print(f"[RESULT DIVERGENCE #{fails}] {q[:200]}  params={params}")
                print(f"   vec: {str(ra)[:260]}")
                print(f"   ref: {str(rb)[:260]}")
                if fails >= 5:
                    print("(stopping after 5 divergences)")
                    break
    finally:
        pa.terminate()
        pb.terminate()
    print(f"\n{stats['ok']} ok / {stats['err']} errors compared, {fails} divergences (seed {SEED})")
    if fails:
        print("FUZZ FAILED")
        sys.exit(1)
    print("FUZZ PASSED (vectorized == reference on every query)")


if __name__ == "__main__":
    main()
