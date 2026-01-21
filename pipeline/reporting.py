from __future__ import annotations

import logging
from pipeline.runner import PipelineRunResult


def log_run_summary(result: PipelineRunResult) -> None:
    # Logs a standard end-of-run summary for a pipeline execution.
    # Keeping this in one place prevents duplicated formatting across scripts/tests.
    log = logging.getLogger("pipeline")

    log.info("")
    log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    log.info(f"Raw input:        {result.input_csv}")
    log.info(f"Bronze rows:      {result.bronze_rows:,}  ->  {result.bronze_out}")
    log.info(f"Silver rows:      {result.silver_rows:,}  ->  {result.silver_out}")
    log.info(f"Quarantined rows: {result.quarantined_rows:,}  ->  {result.quarantine_out}")

    for path in result.gold_outputs:
        log.info(f"Gold produced:    {path}")

    log.info("Done.")
    log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    log.info("")
