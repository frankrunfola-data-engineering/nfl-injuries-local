"""Beginner-friendly entry point.

Run from the repo root:
    python scripts/run_pipeline.py

Optional:
    python scripts/run_pipeline.py data/raw/injuries_raw.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure the repo root is on sys.path so `import pipeline...` works.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from pipeline.runner import run


def main() -> None:
    raw_csv = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    run(raw_csv=raw_csv)


if __name__ == "__main__":
    main()
