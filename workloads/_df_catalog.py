"""Shared df catalog helpers for the ``*_read_delta`` workloads.

df runs each ``delta-forge-cli`` statement as its own session, so a session-scoped
OPEN DELTA TABLE cannot carry across the per-query sessions. Instead REGISTER the
existing Delta tables once (catalog-persistent, 3-part ``zone.schema.table``,
idempotent), and reference them by their qualified name in each query. The query
files are shared with DuckDB / Spark (bare names), so df's copy is qualified here.

Registration is a ONE-TIME catalog DDL tied to dataset creation, NOT per-query
work: ``bench_runner`` runs it once per df session (its catalog phase), before
any measured step, and never unregisters (the next clean-slate boot starts from
an empty catalog, and ``REGISTER DELTA TABLE`` is idempotent on a same-session
re-run). It therefore never appears in the measured query loop.

Each benchmark uses its OWN zone (the zone's storage_root is the benchmark's
``_delta`` dir), so the zones never collide when several read workloads run in one
``./bench`` invocation.
"""
from __future__ import annotations

import re


def df_register_setup(zone: str, schema: str, delta_root: str, tables: list[str]) -> str:
    """CREATE the zone (storage_root = the ``_delta`` dir) + schema, then REGISTER
    each existing Delta table under a LOCATION RELATIVE to the storage_root (just
    the table's folder name). REGISTER is idempotent, so a re-run is harmless."""
    parts = [
        f"CREATE ZONE IF NOT EXISTS {zone} STORAGE_ROOT = '{delta_root}'",
        f"CREATE SCHEMA IF NOT EXISTS {zone}.{schema}",
    ]
    for t in tables:
        parts.append(f"REGISTER DELTA TABLE {zone}.{schema}.{t} LOCATION '{t}'")
    return ";\n".join(parts)


# Keywords that turn the FROM-clause context on / off as we scan a statement.
# ``on`` is deliberately NOT a terminator: a join predicate's identifiers are all
# dotted (alias.col), so leaving the context on lets a trailing comma cross-join
# table (``JOIN b ON (...), c``) still be qualified without any false hits.
_FROM_ON = {"from", "join"}
_CLAUSE_OFF = {
    "where", "group", "order", "having", "limit", "union", "select",
    "intersect", "except", "using", "qualify", "window",
}
# One token at a time: a single-quoted string, an identifier/number word, or any
# other single character (whitespace, punctuation, dot, comma, paren...).
_TOKEN = re.compile(r"'(?:[^']|'')*'|[A-Za-z_][A-Za-z0-9_]*|.", re.S)


def df_qualify(sql: str, tables: list[str], zone: str, schema: str) -> str:
    """Qualify a benchmark table name to its registered 3-part name ONLY where it
    is an actual table reference -- inside a FROM / JOIN clause.

    DeltaForge addresses objects as ``zone.schema.object`` with no 4-part names,
    so once a table is in the query via ``FROM zone.schema.object`` its relation
    name is still ``object`` and an ``object.column`` reference in the body
    resolves to it unchanged. We therefore only rewrite the table name in the
    FROM/JOIN clause and leave every column reference, alias, and string literal
    alone. A small state machine tracks FROM-clause context (FROM/JOIN turn it
    on; WHERE/GROUP/ON/SELECT/... turn it off) with a stack so a subquery's inner
    clauses do not leak into the enclosing FROM list.
    """
    prefix = f"{zone}.{schema}."
    tableset = set(tables)
    toks = [m.group(0) for m in _TOKEN.finditer(sql)]
    n = len(toks)
    out: list[str] = []
    in_from = False
    stack: list[bool] = []
    prev_sig = ""  # last non-whitespace token seen
    for i, t in enumerate(toks):
        if t[:1] == "'":            # string literal -- never touched
            out.append(t)
            prev_sig = t
        elif t == "(":              # subquery: save enclosing FROM context
            stack.append(in_from)
            out.append(t)
            prev_sig = t
        elif t == ")":              # ... and restore it on close
            in_from = stack.pop() if stack else in_from
            out.append(t)
            prev_sig = t
        elif t[:1].isalpha() or t[:1] == "_":
            low = t.lower()
            if low in _FROM_ON:
                in_from = True
                out.append(t)
            elif low in _CLAUSE_OFF:
                in_from = False
                out.append(t)
            elif (in_from and t in tableset
                    # A table reference sits immediately after FROM / JOIN / a
                    # comma. Anything else that happens to equal a table name --
                    # a relation's alias (``store_v1 store``), a derived-table
                    # alias (``(...) store``), an AS alias, or a column -- is NOT
                    # in that position and is left alone.
                    and prev_sig.lower() in ("from", "join", ",")
                    and (i + 1 >= n or toks[i + 1] != ".")):
                out.append(prefix + t)
            else:
                out.append(t)
            prev_sig = t
        else:
            out.append(t)
            if t.strip():           # punctuation is significant; whitespace is not
                prev_sig = t
    return "".join(out)
