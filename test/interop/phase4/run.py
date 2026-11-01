#!/usr/bin/env python3
from __future__ import annotations

import importlib
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def load_duckdb_module():
    """优先使用 Python DuckDB；缺失时允许回退到官方 CLI。"""
    try:
        return importlib.import_module("duckdb")
    except Exception:
        return None


def load_pyarrow_parquet():
    """PyArrow 是附加互操作证据，不是本阶段 DuckDB 对标的硬前置。"""
    try:
        return importlib.import_module("pyarrow.parquet")
    except Exception:
        return None


def read_with_duckdb(path: Path) -> list[tuple[int, str | None]]:
    """用 Python 模块或 DuckDB CLI 读取 fssql writer 产物。"""
    module = load_duckdb_module()
    if module is not None:
        conn = module.connect()
        try:
            return conn.execute(
                "select id, name from read_parquet(?) order by id",
                [str(path)],
            ).fetchall()
        finally:
            conn.close()

    cli = shutil.which("duckdb")
    if cli is None:
        print(
            "Phase 4 Parquet writer interop BLOCKED: DuckDB Python module and CLI are both unavailable",
            file=sys.stderr,
        )
        raise SystemExit(2)
    escaped = str(path).replace("'", "''")
    query = (
        "select id, coalesce(name, '<NULL>') "
        f"from read_parquet('{escaped}') order by id;"
    )
    result = subprocess.run(
        [cli, "-csv", "-noheader", "-c", query],
        check=True,
        capture_output=True,
        text=True,
    )
    rows: list[tuple[int, str | None]] = []
    for line in result.stdout.splitlines():
        raw_id, raw_name = line.split(",", 1)
        rows.append((int(raw_id), None if raw_name == "<NULL>" else raw_name))
    return rows


def main() -> None:
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

        duckdb_rows = read_with_duckdb(path)
        if duckdb_rows != expected:
            raise AssertionError(f"DuckDB rows={duckdb_rows!r}, want={expected!r}")

        evidence: dict[str, object] = {
            "file": "fssql.parquet",
            "rows": len(expected),
            "duckdb": duckdb_rows,
        }
        pq = load_pyarrow_parquet()
        if pq is not None:
            table = pq.read_table(path)
            pyarrow_rows = list(zip(table.column("id").to_pylist(), table.column("name").to_pylist()))
            if pyarrow_rows != expected:
                raise AssertionError(f"PyArrow rows={pyarrow_rows!r}, want={expected!r}")
            evidence["pyarrow"] = pyarrow_rows
        else:
            evidence["pyarrow"] = "not installed (optional)"

        print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
