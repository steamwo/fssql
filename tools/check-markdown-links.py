"""Check relative Markdown link targets in the working tree."""

from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote, urlsplit


INLINE = re.compile(r"\]\(\s*(<[^>]+>|[^\s)]+)")
REFERENCE = re.compile(r"^ {0,3}\[(?!\^)[^\]]+\]:\s*(<[^>]+>|\S+)")
FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")


def missing_links(root, document):
    fence = ""
    for number, line in enumerate((root / document).read_text(encoding="utf-8-sig").splitlines(), 1):
        marker = FENCE.match(line)
        if fence:
            if marker and marker[1][0] == fence[0] and len(marker[1]) >= len(fence) and not marker[2].strip():
                fence = ""
            continue
        if marker:
            fence = marker[1]
            continue
        destinations = INLINE.findall(line) + REFERENCE.findall(line)
        for destination in destinations:
            target = urlsplit(destination.strip("<>"))
            if target.scheme or target.netloc or not target.path or target.path.startswith("/"):
                continue
            if not (root / document.parent / unquote(target.path)).exists():
                yield f"{document}:{number}: missing {destination}"


def main():
    root = Path(__file__).resolve().parents[1]
    files = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", "--", "*.md"], cwd=root,
    )
    documents = sorted({Path(name.decode("utf-8")) for name in files.split(b"\x00") if name})
    errors = [error for document in documents if (root / document).is_file() for error in missing_links(root, document)]
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("Markdown link targets: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
