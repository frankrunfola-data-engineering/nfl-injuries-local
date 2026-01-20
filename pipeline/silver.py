"""SILVER stage (bronze -> silver + quarantine).

Filesystem contract (local):
- Input:  data/bronze/injuries_bronze.csv
- Output: data/silver/injuries_silver.csv
- Output: data/quarantine/injuries_quarantine.csv

Silver is where we start to care about data quality.
We keep it intentionally simple and readable:
- Schema validation via a tiny JSON file (schema/injuries_schema.json)
- Null/blank checks for required fields
- Type checks (season/week ints)
- Basic guardrail (week range)
- Report date parse (if present)
- Duplicate record check (primary_key in the schema)

Bad rows are NOT dropped silently:
- They are written to quarantine with a `quarantine_reason`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def _is_blank(series: pd.Series) -> pd.Series:
    """True where value is NA or an empty/whitespace string."""

    s = series.copy()
    # Treat non-strings as strings only for blank checking
    s = s.astype("string")
    return s.isna() | (s.str.strip() == "")


def _load_schema(schema_path: Path) -> dict:
    with schema_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _append_reason(reason: pd.Series, mask: pd.Series, msg: str) -> pd.Series:
    """Append msg to reason where mask is True."""

    out = reason.copy()
    needs_sep = (out != "") & mask
    out = out.where(~needs_sep, out + "; ")
    out = out.where(~mask, out + msg)
    return out


def run(
    bronze_path: Path,
    silver_out: Path,
    quarantine_out: Path,
    schema_path: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(bronze_path)

    schema = _load_schema(schema_path)
    required_cols = list(schema.get("required", {}).keys())
    optional_cols = list(schema.get("optional", {}).keys())
    primary_key = list(schema.get("primary_key", []))
    week_range = schema.get("week_range", [1, 22])

    # File-level contract: if required columns are missing, fail loudly.
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            "Input is missing required columns: " + ", ".join(missing)
        )

    keep_cols = required_cols + [c for c in optional_cols if c in df.columns]
    df = df[keep_cols].copy()

    reason = pd.Series("", index=df.index, dtype="string")

    # 1) Required fields cannot be blank
    any_required_blank = pd.Series(False, index=df.index)
    for c in required_cols:
        any_required_blank |= _is_blank(df[c])
    reason = _append_reason(reason, any_required_blank, "missing_required")

    # 2) season/week must be ints
    season_num = pd.to_numeric(df["season"], errors="coerce")
    week_num = pd.to_numeric(df["week"], errors="coerce")

    invalid_season = season_num.isna()
    invalid_week = week_num.isna()
    reason = _append_reason(reason, invalid_season, "invalid_season")
    reason = _append_reason(reason, invalid_week, "invalid_week")

    # 3) week in range (default 1..22)
    lo, hi = int(week_range[0]), int(week_range[1])
    week_out_of_range = (~invalid_week) & ((week_num < lo) | (week_num > hi))
    reason = _append_reason(reason, week_out_of_range, "week_out_of_range")

    # 4) team cannot be blank (duplicate of required check, but clearer reason)
    blank_team = _is_blank(df["team"])
    reason = _append_reason(reason, blank_team, "blank_team")

    # 5) report_date parse (optional)
    if "report_date" in df.columns:
        report_is_blank = _is_blank(df["report_date"])
        parsed = pd.to_datetime(df["report_date"], errors="coerce")
        invalid_date = (~report_is_blank) & parsed.isna()
        reason = _append_reason(reason, invalid_date, "invalid_report_date")

    # 6) duplicate records (primary key)
    if primary_key and all(c in df.columns for c in primary_key):
        dup_mask = df.duplicated(subset=primary_key, keep="first")
        reason = _append_reason(reason, dup_mask, "duplicate_record")

    df["quarantine_reason"] = reason
    quarantine_mask = reason != ""

    quarantine_df = df.loc[quarantine_mask].copy()
    silver_df = df.loc[~quarantine_mask].copy()

    # Normalize types for Silver
    silver_df["season"] = pd.to_numeric(silver_df["season"], errors="raise").astype(int)
    silver_df["week"] = pd.to_numeric(silver_df["week"], errors="raise").astype(int)

    if "report_date" in silver_df.columns:
        parsed = pd.to_datetime(silver_df["report_date"], errors="coerce")
        silver_df["report_date"] = parsed.dt.strftime("%Y-%m-%d").fillna("")

    # Write outputs
    silver_out.parent.mkdir(parents=True, exist_ok=True)
    quarantine_out.parent.mkdir(parents=True, exist_ok=True)

    silver_df.to_csv(silver_out, index=False)
    quarantine_df.to_csv(quarantine_out, index=False)

    return silver_df, quarantine_df
