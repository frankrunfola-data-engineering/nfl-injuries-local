"""
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
File:    gold.py
Author:  Frank Runfola
Date:    11/1/2025
-------------------------------------------------------------------------------
GOLD stage (silver -> gold).

Filesystem contract (local):
- Input:  data/silver/injuries_silver.csv
- Output: data/gold/injuries_by_team_week.csv
- Output: data/gold/injuries_by_position.csv

Gold is analytics-ready output (small, denormalized, aggregated).
Keep it small so you can eyeball the results.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def run(silver_path: Path, gold_dir: Path) -> list[str]:
    df = pd.read_csv(silver_path)

    gold_dir.mkdir(parents=True, exist_ok=True)

    outputs: list[str] = []

    # 1) Count injuries by team/week
    if {"team", "week", "season"}.issubset(df.columns):
        by_team_week = (
            df.groupby(["season", "week", "team"], dropna=False)
            .size()
            .reset_index(name="injury_count")
            .sort_values(["season", "week", "team"])
        )
        out1 = gold_dir / "injuries_by_team_week.csv"
        by_team_week.to_csv(out1, index=False)
        outputs.append(out1.name)

    # 2) Count injuries by position
    if "position" in df.columns:
        by_position = (
            df.groupby(["position"], dropna=False)
            .size()
            .reset_index(name="injury_count")
            .sort_values(["injury_count", "position"], ascending=[False, True])
        )
        out2 = gold_dir / "injuries_by_position.csv"
        by_position.to_csv(out2, index=False)
        outputs.append(out2.name)

    return outputs
