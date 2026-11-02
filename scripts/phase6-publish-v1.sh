#!/usr/bin/env bash
set -euo pipefail

release_tag="${RELEASE_TAG:-v1.0}"
release_title="${RELEASE_TITLE:-v1.0 Stable}"
repository="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
release_sha="${RELEASE_SHA:?RELEASE_SHA is required}"
notes_file="${RELEASE_NOTES_FILE:-artifacts/phase-6/acceptance-report.md}"

python3 - "$notes_file" <<'PY'
from pathlib import Path
import re
import sys

state = Path("docs/machine/project-state.yaml").read_text(encoding="utf-8")
if not re.search(r"^current_phase:\s*6\s*$", state, re.MULTILINE):
    raise SystemExit("release blocked: current_phase is not 6")
if not re.search(r"^\s*phase_6:\s*ACCEPTED\s*$", state, re.MULTILINE):
    raise SystemExit("release blocked: phase_6 is not ACCEPTED")

required = [
    Path("artifacts/phase-6/acceptance-report.md"),
    Path("artifacts/phase-6/benchmark-report.md"),
    Path("artifacts/phase-6/memory-report.md"),
    Path("artifacts/phase-6/failure-report.md"),
    Path("artifacts/phase-6/known-limitations.md"),
    Path(sys.argv[1]),
]
for path in required:
    if not path.is_file() or path.stat().st_size == 0:
        raise SystemExit(f"release blocked: missing or empty evidence {path}")
PY

head_sha="$(git rev-parse HEAD)"
if [[ "$head_sha" != "$release_sha" ]]; then
  echo "release blocked: checkout HEAD=$head_sha, release SHA=$release_sha" >&2
  exit 1
fi

if [[ "${PUBLISH_DRY_RUN:-0}" == "1" ]]; then
  echo "Phase 6 v1 publisher preflight: PASS ($release_tag @ $release_sha)"
  exit 0
fi

: "${GH_TOKEN:?GH_TOKEN is required for publication}"
command -v gh >/dev/null

if gh release view "$release_tag" --repo "$repository" >/dev/null 2>&1; then
  existing_sha="$(gh api "repos/$repository/commits/$release_tag" --jq '.sha')"
  if [[ "$existing_sha" != "$release_sha" ]]; then
    echo "release blocked: $release_tag already resolves to $existing_sha, expected $release_sha" >&2
    exit 1
  fi
  echo "$release_tag already published at verified commit $release_sha"
  exit 0
fi

gh release create "$release_tag" \
  --repo "$repository" \
  --target "$release_sha" \
  --title "$release_title" \
  --notes-file "$notes_file"

published_sha="$(gh api "repos/$repository/commits/$release_tag" --jq '.sha')"
if [[ "$published_sha" != "$release_sha" ]]; then
  echo "release verification failed: $release_tag resolves to $published_sha, expected $release_sha" >&2
  exit 1
fi

echo "Published $release_tag at verified commit $release_sha"
