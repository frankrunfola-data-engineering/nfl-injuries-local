"""
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
File:    runner.py
Author:  Frank Runfola
Date:    11/1/2025
-------------------------------------------------------------------------------
Pipeline runner (raw -> bronze -> silver -> gold).

This keeps orchestration in one place so the stage modules stay focused.

Filesystem contract (local repo):
- data/raw/        input CSVs
- data/bronze/     standardized raw copy
- data/silver/     validated clean rows
- data/quarantine/ rejected rows + reasons (pipeline keeps running)
- data/gold/       aggregations

Notes:
- This module returns structured results and does not configure logging.
The CLI script is responsible for logging setup and printing.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pipeline.raw import run as run_raw_csv
from pipeline.bronze import run as run_bronze
from pipeline.gold import run as run_gold
from pipeline.silver import run as run_silver
from pipeline.paths import build_paths, ensure_dirs


@dataclass(frozen=True)
class PipelineRunResult:
    input_csv: Path
    bronze_rows: int
    silver_rows: int
    quarantined_rows: int
    gold_outputs: list[Path]
    bronze_out: Path
    silver_out: Path
    quarantine_out: Path



def run() -> PipelineRunResult:
    p = build_paths()
    ensure_dirs(p)

    input_csv = run_raw_csv(p.raw_dir)
    
    ##################################################
    # Run bronze stage
    ##################################################
    bronze_df = run_bronze(input_csv, p.bronze_out)
    
    ##################################################
    # Run silver stage
    ##################################################
    silver_df, quarantine_df = run_silver(
        p.bronze_out,
        p.silver_out,
        p.quarantine_out,
        p.schema_path,
    )

    ##################################################
    # Run gold stage
    ##################################################

    output_names = run_gold(p.silver_out, p.gold_dir)
    gold_paths = [p.gold_dir / name for name in output_names]

    return PipelineRunResult(
        input_csv=input_csv,
        bronze_rows=len(bronze_df),
        silver_rows=len(silver_df),
        quarantined_rows=len(quarantine_df),
        gold_outputs=gold_paths,
        bronze_out=p.bronze_out,
        silver_out=p.silver_out,
        quarantine_out=p.quarantine_out,
    )
