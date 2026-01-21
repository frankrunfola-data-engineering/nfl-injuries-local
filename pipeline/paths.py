# pipeline/paths.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger("pipeline")

def repo_root() -> Path:
    # This file is: <repo>/pipeline/paths.py
    # parents[1] -> <repo>
    return Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class PipelinePaths:
    root: Path
    raw_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    quarantine_dir: Path
    schema_path: Path

    # Common filenames (so they aren't duplicated everywhere)
    bronze_out: Path
    silver_out: Path
    quarantine_out: Path


def build_paths(root: Path | None = None) -> PipelinePaths:
    r = root or repo_root()

    raw_dir = r / "data" / "raw"
    bronze_dir = r / "data" / "bronze"
    silver_dir = r / "data" / "silver"
    gold_dir = r / "data" / "gold"
    quarantine_dir = r / "data" / "quarantine"
    schema_path = r / "schema" / "injuries_schema.json"

    return PipelinePaths(
        root=r,
        raw_dir=raw_dir,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        quarantine_dir=quarantine_dir,
        schema_path=schema_path,
        bronze_out=bronze_dir / "injuries_bronze.csv",
        silver_out=silver_dir / "injuries_silver.csv",
        quarantine_out=quarantine_dir / "injuries_quarantine.csv",
    )
    
def ensure_dirs(paths: PipelinePaths) -> None:
    logger.debug("Ensuring pipeline directories exist under: %s", paths.root)
    paths.raw_dir.mkdir(parents=True, exist_ok=True)
    paths.bronze_dir.mkdir(parents=True, exist_ok=True)
    paths.silver_dir.mkdir(parents=True, exist_ok=True)
    paths.gold_dir.mkdir(parents=True, exist_ok=True)
    paths.quarantine_dir.mkdir(parents=True, exist_ok=True)
    paths.schema_path.parent.mkdir(parents=True, exist_ok=True)
    
    if not paths.schema_path.exists():
        # Not fatal (you might generate it or commit it later), but likely important.
        logger.warning("Schema file not found: %s", paths.schema_path)