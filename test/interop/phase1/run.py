#!/usr/bin/env python3
from __future__ import annotations

import importlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path

REQUIRED = {
    "pyarrow": "PyArrow",
    "duckdb": "DuckDB",
    "polars": "Polars",
    "pyspark": "Spark",
}


def require_engines() -> dict[str, object]:
    loaded: dict[str, object] = {}
    missing: list[str] = []
    for module, label in REQUIRED.items():
        try:
            loaded[module] = importlib.import_module(module)
        except Exception as exc:  # environment evidence, not a skipped success
            missing.append(f"{label} ({module}): {exc}")
    if missing:
        print("Phase 1 interop BLOCKED: required engines are missing", file=sys.stderr)
        for item in missing:
            print(f"- {item}", file=sys.stderr)
        raise SystemExit(2)
    return loaded


def count_pyarrow(pa, pq, path: Path) -> int:
    return pq.read_table(path).num_rows


def count_duckdb(duckdb, path: Path) -> int:
    query_path = str(path / "*.parquet") if path.is_dir() else str(path)
    conn = duckdb.connect()
    try:
        row = conn.execute("select count(*) from read_parquet(?)", [query_path]).fetchone()
        if row is None:
            raise AssertionError(f"duckdb returned no count row for {query_path}")
        return int(row[0])
    finally:
        conn.close()


def count_polars(pl, path: Path) -> int:
    query_path = str(path / "*.parquet") if path.is_dir() else str(path)
    return int(pl.read_parquet(query_path).height)


def count_spark(spark, path: Path) -> int:
    return int(spark.read.parquet(str(path)).count())


def assert_count(label: str, got: int, want: int) -> None:
    if got != want:
        raise AssertionError(f"{label}: row count {got}, want {want}")


def generate_corpus(corpus_dir: Path, generated: Path) -> None:
    # run.py stdout is machine-readable release evidence. Keep generator
    # diagnostics visible without contaminating the JSON stream.
    subprocess.run(
        [sys.executable, str(corpus_dir / "generate.py"), "--out", str(generated)],
        check=True,
        stdout=sys.stderr,
    )


def main() -> None:
    engines = require_engines()
    pa = engines["pyarrow"]
    pq = importlib.import_module("pyarrow.parquet")
    duckdb = engines["duckdb"]
    pl = engines["polars"]
    SparkSession = importlib.import_module("pyspark.sql").SparkSession

    here = Path(__file__).resolve().parent
    corpus_dir = here.parents[1] / "corpus" / "phase1"
    generated = corpus_dir / "generated"
    generate_corpus(corpus_dir, generated)

    spark = SparkSession.builder.master("local[1]").appName("fssql-phase1-interop").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    evidence: dict[str, object] = {"canonical_corpus": {}, "producer_grid": {}}
    try:
        canonical = {
            "flat_dictionary.parquet": 4,
            "nested.parquet": 4,
            "decimal128.parquet": 2,
        }
        for relative, expected in canonical.items():
            path = generated / relative
            counts = {
                "pyarrow": count_pyarrow(pa, pq, path),
                "duckdb": count_duckdb(duckdb, path),
                "polars": count_polars(pl, path),
                "spark": count_spark(spark, path),
            }
            for engine, got in counts.items():
                assert_count(f"{engine}:{relative}", got, expected)
            evidence["canonical_corpus"][relative] = counts

        hive_root = generated / "hive"
        hive_counts = {
            "pyarrow": pq.ParquetDataset(hive_root).read().num_rows,
            "duckdb": count_duckdb(duckdb, hive_root / "**" / "*.parquet"),
            "polars": int(pl.scan_parquet(str(hive_root / "**" / "*.parquet")).collect().height),
            "spark": int(spark.read.option("basePath", str(hive_root)).parquet(str(hive_root / "year=2026" / "*" / "*.parquet")).count()),
        }
        for engine, got in hive_counts.items():
            assert_count(f"{engine}:hive", int(got), 4)
        evidence["canonical_corpus"]["hive"] = hive_counts

        with tempfile.TemporaryDirectory(prefix="fssql-phase1-interop-") as temp:
            temp_root = Path(temp)
            rows = [(1, "alpha"), (2, "beta"), (3, None)]

            pyarrow_path = temp_root / "pyarrow.parquet"
            pq.write_table(pa.table({"id": [r[0] for r in rows], "name": [r[1] for r in rows]}), pyarrow_path)

            duckdb_path = temp_root / "duckdb.parquet"
            duck_conn = duckdb.connect()
            try:
                duck_conn.execute("create table phase1_interop(id bigint, name varchar)")
                duck_conn.execute("insert into phase1_interop values (1, 'alpha'), (2, 'beta'), (3, NULL)")
                target = duckdb_path.as_posix().replace("'", "''")
                duck_conn.execute(f"copy phase1_interop to '{target}' (format parquet)")
            finally:
                duck_conn.close()

            polars_path = temp_root / "polars.parquet"
            pl.DataFrame({"id": [1, 2, 3], "name": ["alpha", "beta", None]}).write_parquet(polars_path)

            spark_path = temp_root / "spark-parquet"
            spark.createDataFrame(rows, schema="id long, name string").write.mode("overwrite").parquet(str(spark_path))

            producer_paths = {
                "pyarrow": pyarrow_path,
                "duckdb": duckdb_path,
                "polars": polars_path,
                "spark": spark_path,
            }
            for producer, path in producer_paths.items():
                counts = {
                    "pyarrow": count_pyarrow(pa, pq, path),
                    "duckdb": count_duckdb(duckdb, path),
                    "polars": count_polars(pl, path),
                    "spark": count_spark(spark, path),
                }
                for reader, got in counts.items():
                    assert_count(f"producer={producer},reader={reader}", got, 3)
                evidence["producer_grid"][producer] = counts
    finally:
        spark.stop()

    print(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
