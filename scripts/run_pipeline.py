"""
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
File:    run_pipeline.py
Author:  Frank Runfola
Date:    11/1/2025
-------------------------------------------------------------------------------
Description:
  One-command runner for the tiny Medallion pipeline.
  This is OPTIONAL — you can run the notebooks instead.
  But scripts are nice for CI or quick demos.
-------------------------------------------------------------------------------
Running Instructions:
  python scripts/run_pipeline.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Ensure the repo root is on sys.path so `import pipeline...` works.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from pipeline.runner import run
from pipeline.reporting import log_run_summary


def main() -> None:
    #sets up handlers/formatting once for the process.
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    result = run()
    log_run_summary(result)# uses logging.getLogger("pipeline"), which inherits the config.


if __name__ == "__main__":
    main()
