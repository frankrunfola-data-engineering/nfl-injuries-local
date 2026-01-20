"""Pipeline runner (raw -> bronze -> silver -> gold).

This keeps orchestration in one place so the stage modules stay focused.

Filesystem contract (local repo):
- data/raw/        input CSVs
- data/bronze/     standardized raw copy
- data/silver/     validated clean rows
- data/quarantine/ rejected rows + reasons (pipeline keeps running)
- data/gold/       aggregations

You can run this directly:
    python scripts/run_pipeline.py
"""

from __future__ import annotations

import logging
from pathlib import Path

from pipeline.bronze import run as run_bronze
from pipeline.gold import run as run_gold
from pipeline.silver import run as run_silver

logger = logging.getLogger(__name__)


def _repo_root() -> Path:
    # This file lives at: <repo>/pipeline/runner.py
    return Path(__file__).resolve().parents[1]


def _pick_raw_csv(raw_dir: Path) -> Path:
    # Prefer the main training file if it exists.
    preferred = raw_dir / "injuries_raw.csv"
    if preferred.exists():
        return preferred

    csvs = sorted(raw_dir.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError("No CSV found in data/raw/. Put a file there and re-run.")
    return csvs[0]


def run(raw_csv: Path | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    root = _repo_root()
    raw_dir = root / "data" / "raw"
    bronze_dir = root / "data" / "bronze"
    silver_dir = root / "data" / "silver"
    gold_dir = root / "data" / "gold"
    quarantine_dir = root / "data" / "quarantine"
    schema_path = root / "schema" / "injuries_schema.json"

    input_csv = raw_csv or _pick_raw_csv(raw_dir)

    bronze_out = bronze_dir / "injuries_bronze.csv"
    silver_out = silver_dir / "injuries_silver.csv"
    quarantine_out = quarantine_dir / "injuries_quarantine.csv"

    logger.info("")
    logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    logger.info(f"Raw input:        {input_csv}")

    bronze_df = run_bronze(input_csv, bronze_out)
    logger.info(f"Bronze rows:      {len(bronze_df):,}  ->  {bronze_out}")

    silver_df, quarantine_df = run_silver(bronze_out, silver_out, quarantine_out, schema_path)
    logger.info(f"Silver rows:      {len(silver_df):,}  ->  {silver_out}")
    logger.info(f"Quarantined rows: {len(quarantine_df):,}  ->  {quarantine_out}")

    outputs = run_gold(silver_out, gold_dir)
    for name in outputs:
        logger.info(f"Gold produced:    {gold_dir / name}")

    logger.info("Done.")
    logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    logger.info("")
