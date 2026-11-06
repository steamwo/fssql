from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock


RUN_PY = Path(__file__).with_name("run.py")
spec = importlib.util.spec_from_file_location("parquet_interop_run", RUN_PY)
if spec is None or spec.loader is None:
    raise RuntimeError(f"cannot load {RUN_PY}")
run_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run_module)


class GenerateCorpusOutputContractTest(unittest.TestCase):
    def test_generator_stdout_is_routed_to_stderr(self) -> None:
        corpus_dir = Path("/tmp/corpus")
        generated = Path("/tmp/generated")
        with mock.patch.object(run_module.subprocess, "run") as run:
            run_module.generate_corpus(corpus_dir, generated)

        run.assert_called_once_with(
            [
                sys.executable,
                str(corpus_dir / "generate.py"),
                "--out",
                str(generated),
            ],
            check=True,
            stdout=sys.stderr,
        )


if __name__ == "__main__":
    unittest.main()
