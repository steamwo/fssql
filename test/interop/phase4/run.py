#!/usr/bin/env python3
from __future__ import annotations

import importlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path

REQUIRED = {
    "duckdb": "DuckDB",
    "pyarrow": "PyArrow",
}


def require_engines() -> dict[str, object]:
    """加载外部 reader；缺失必须显式 BLOCKED，不能当作跳过成功。"""
    loaded: dict[str, object] = {}
    missing: list[str] = []
    for module, label in REQUIRED.items():
        try:
            loaded[module] = importlib.import_module(module)
        except Exception as exc:
            missing.append(f"{label} ({module}): {exc}")
    if missing:
        print("Phase 4 Parquet writer interop BLOCKED: required readers are missing", file=sys.stderr)
        for item in missing:
            print(f"- {item}", file=sys.stderr)
        raise SystemExit(2)
    return loaded


def main() -> None:
    engines = require_engines()
    duckdb = engines["duckdb"]
    pq = importlib.import_module("pyarrow.parquet")
    here = Path(__file__).resolve().parent
    repo = here.parents[2]
    expected = [(1, "alpha"), (2, "beta"), (3, None)]

    with tempfile.TemporaryDirectory(prefix="fssql-phase4-parquet-") as temp:
        path = Path(temp) / "fssql.parquet"
        subprocess.run(
            ["go", "run", "./test/interop/phase4/generator", str(path)],
            cwd=repo,
            check=True,
        )

        table = pq.read_table(path)
        pyarrow_rows = list(zip(table.column("id").to_pylist(), table.column("name").to_pylist()))
        if pyarrow_rows != expected:
            raise AssertionError(f"PyArrow rows={pyarrow_rows!r}, want={expected!r}")

        conn = duckdb.connect()
        try:
            duckdb_rows = conn.execute(
                "select id, name from read_parquet(?) order by id",
                [str(path)],
            ).fetchall()
        finally:
            conn.close()
        if duckdb_rows != expected:
            raise AssertionError(f"DuckDB rows={duckdb_rows!r}, want={expected!r}")

        print(json.dumps({
            "file": "fssql.parquet",
            "rows": len(expected),
            "pyarrow": pyarrow_rows,
            "duckdb": duckdb_rows,
        }, indent=2))


if __name__ == "__main__":
    main()
