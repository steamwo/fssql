#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def write_table(path: Path, table: pa.Table, *, row_group_size: int = 2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table,
        path,
        compression="snappy",
        use_dictionary=True,
        row_group_size=row_group_size,
        version="2.6",
        data_page_version="1.0",
        write_statistics=True,
    )


def generate(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)

    flat_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("category", pa.string()),
            pa.field("amount", pa.decimal128(18, 2)),
            pa.field("ts", pa.timestamp("us")),
        ]
    )
    flat = pa.Table.from_pylist(
        [
            {"id": 1, "category": "alpha", "amount": Decimal("12.34"), "ts": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)},
            {"id": 2, "category": "alpha", "amount": Decimal("-0.01"), "ts": datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc).replace(tzinfo=None)},
            {"id": 3, "category": "beta", "amount": None, "ts": None},
            {"id": 4, "category": None, "amount": Decimal("9999999999999999.99"), "ts": datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)},
        ],
        schema=flat_schema,
    )
    write_table(root / "flat_dictionary.parquet", flat)

    profile_type = pa.struct([pa.field("name", pa.string()), pa.field("age", pa.int32())])
    item_type = pa.struct(
        [
            pa.field("sku", pa.string(), nullable=False),
            pa.field("qty", pa.int32(), nullable=False),
            pa.field("note", pa.string()),
        ]
    )
    nested_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("profile", profile_type),
            pa.field("items", pa.list_(item_type)),
        ]
    )
    nested = pa.Table.from_pylist(
        [
            {"id": 1, "profile": {"name": "one", "age": 10}, "items": None},
            {"id": 2, "profile": None, "items": []},
            {"id": 3, "profile": {"name": "three", "age": None}, "items": [None, {"sku": "A", "qty": 1, "note": None}, {"sku": "B", "qty": 2, "note": "ok"}]},
            {"id": 4, "profile": {"name": None, "age": 40}, "items": [{"sku": "C", "qty": 3, "note": "last"}]},
        ],
        schema=nested_schema,
    )
    write_table(root / "nested.parquet", nested)

    hive_schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("value", pa.string())])
    write_table(root / "hive" / "year=2026" / "month=01" / "part-0.parquet", pa.Table.from_pylist([{"id": 1, "value": "jan-a"}, {"id": 2, "value": "jan-b"}], schema=hive_schema))
    write_table(root / "hive" / "year=2026" / "month=02" / "part-0.parquet", pa.Table.from_pylist([{"id": 3, "value": "feb-a"}, {"id": 4, "value": "feb-b"}], schema=hive_schema))

    write_table(root / "union" / "a.parquet", pa.Table.from_pylist([{"id": 1, "a": "x"}, {"id": 2, "a": "y"}], schema=pa.schema([pa.field("id", pa.int64()), pa.field("a", pa.string())])))
    write_table(root / "union" / "b.parquet", pa.Table.from_pylist([{"id": 3, "b": 30}, {"id": 4, "b": 40}], schema=pa.schema([pa.field("id", pa.int64()), pa.field("b", pa.int32())])))

    decimal128_schema = pa.schema([pa.field("id", pa.int64()), pa.field("amount", pa.decimal128(20, 4))])
    decimal128 = pa.Table.from_pylist(
        [
            {"id": 1, "amount": Decimal("1234567890123456.7890")},
            {"id": 2, "amount": Decimal("-9999999999999999.0001")},
        ],
        schema=decimal128_schema,
    )
    write_table(root / "decimal128.parquet", decimal128)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the Phase 1 Parquet correctness/interop corpus")
    parser.add_argument("--out", type=Path, default=Path(__file__).with_name("generated"))
    args = parser.parse_args()
    generate(args.out)
    print(args.out.resolve())


if __name__ == "__main__":
    main()
