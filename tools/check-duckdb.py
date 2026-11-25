"""Require each local DuckDB reference test to run and pass."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


TESTS = (
    "TestDuckDBReferenceMatchesDeviationRegistry",
    "TestDuckDBDifferentialCorpus",
    "TestDuckDBDeepDifferential",
    "TestDuckDBFileInterop",
    "TestDuckDBParquetTypeInterop",
    "TestParquetZSTDFileInterop",
    "TestDuckDBTypedWorkload",
    "TestDuckDBTypedNumericCasts",
    "TestDuckDBTypedSetOperations",
    "TestDuckDBTypedBulk",
    "TestDuckDBTypedTemporal",
)

PR_TESTS = (
    "TestDuckDBTypedWorkload",
    "TestDuckDBTypedSetOperations",
    "TestDuckDBFileInterop",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pr", action="store_true", help="run the pull request test subset")
    args = parser.parse_args()
    tests = PR_TESTS if args.pr else TESTS
    root = Path(__file__).resolve().parents[1]
    pattern = "^(" + "|".join(tests) + ")$"
    command = ["go", "test", "-count=1", "-timeout=25m", "-json", "./test/compat", "-run", pattern]
    env = dict(os.environ, FSSQL_REQUIRE_DUCKDB="1", FSSQL_PYTHON=sys.executable)
    env.pop("FSSQL_TYPED_WORKLOAD", None)
    passed = set()
    skipped = set()
    with subprocess.Popen(command, cwd=root, env=env, stdout=subprocess.PIPE, encoding="utf-8") as runner:
        for line in runner.stdout:
            event = json.loads(line)
            output = event.get("Output")
            if output:
                print(output, end="", flush=True)
            test = event.get("Test")
            if test and event["Action"] == "pass":
                passed.add(test)
            if test and event["Action"] == "skip":
                skipped.add(test)
        status = runner.wait()

    missing = sorted(set(tests) - passed)
    if skipped:
        print("Skipped DuckDB tests: " + ", ".join(sorted(skipped)), file=sys.stderr)
    if missing:
        print("DuckDB tests without a pass: " + ", ".join(missing), file=sys.stderr)
    return status or int(bool(skipped or missing))


if __name__ == "__main__":
    sys.exit(main())
