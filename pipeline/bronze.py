"""
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
File:    bronze.py
Author:  Frank Runfola
Date:    11/1/2025
-------------------------------------------------------------------------------
BRONZE stage (raw -> bronze).

Filesystem contract (local):
- Input:  data/raw/*.csv
- Output: data/bronze/injuries_bronze.csv

Bronze is intentionally boring:
- Read the CSV
- Normalize column names (lowercase + underscores)
- Trim whitespace from string cells
- Write out a standardized copy

No heavy validation here. We do that in Silver.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


def _normalize_col(name: str) -> str:
    """Convert 'Report Date' -> 'report_date', 'practice-status' -> 'practice_status'."""

    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def _trim_string_cells(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace from any object/string columns."""

    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            out[col] = out[col].astype(str).str.strip()
            # Keep real NaNs as NaN (not the string 'nan')
            out.loc[out[col].str.lower() == "nan", col] = pd.NA
    return out


def run(raw_csv: Path, bronze_out: Path) -> pd.DataFrame:
    """Run Bronze stage and return the Bronze DataFrame."""

    df = pd.read_csv(raw_csv)
    df.columns = [_normalize_col(c) for c in df.columns]
    df = _trim_string_cells(df)

    bronze_out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(bronze_out, index=False)
    return df
