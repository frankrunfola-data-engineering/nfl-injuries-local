from pathlib import Path

def run(raw_dir: Path) -> Path:
    preferred = raw_dir / "injuries_raw.csv"
    if preferred.exists():
        return preferred

    csvs = sorted(raw_dir.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError("No CSV found in data/raw/. Put a file there and re-run.")
    return csvs[0]
