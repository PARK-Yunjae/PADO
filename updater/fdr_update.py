"""
FDR-based market data refresh for ClosingBell.
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from config import GLOBAL_CSV, MAPPING_CSV, OHLCV_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("fdr_update")

GLOBAL_DIR = GLOBAL_CSV.parent
GLOBAL_START_DATE = "2016-01-01"
GLOBAL_SPECS = {
    "kospi": {
        "symbols": ["KS11"],
        "close_col": "kospi_close",
        "change_col": "kospi_change_pct",
    },
    "kosdaq": {
        "symbols": ["KQ11"],
        "close_col": "kosdaq_close",
        "change_col": "kosdaq_change_pct",
    },
    "nasdaq": {
        "symbols": ["IXIC"],
        "close_col": "nasdaq_close",
        "change_col": "nasdaq_change_pct",
    },
    "sp500": {
        "symbols": ["US500"],
        "close_col": "sp500_close",
        "change_col": "sp500_change_pct",
    },
    "dow": {
        "symbols": ["DJI"],
        "close_col": "dow_close",
        "change_col": "dow_change_pct",
    },
    "usdkrw": {
        "symbols": ["USD/KRW"],
        "close_col": "usdkrw_close",
        "change_col": "usdkrw_change_pct",
    },
    "vix": {
        "symbols": ["VIX"],
        "close_col": "vix_close",
        "change_col": "vix_change_pct",
    },
    "dff": {
        "symbols": ["FRED:DFF", "DFF"],
        "close_col": "dff_close",
        "change_col": "dff_change_pct",
    },
    "t10y2y": {
        "symbols": ["FRED:T10Y2Y", "T10Y2Y"],
        "close_col": "t10y2y_close",
        "change_col": "t10y2y_change_pct",
    },
}


def _load_global_merged() -> pd.DataFrame:
    if not GLOBAL_CSV.exists():
        return pd.DataFrame(columns=["date"])
    frame = pd.read_csv(GLOBAL_CSV)
    if frame.empty:
        return pd.DataFrame(columns=["date"])
    frame.columns = [str(col).strip().lower() for col in frame.columns]
    if "date" not in frame.columns:
        raise ValueError("global_merged.csv missing date column")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return frame


def _fetch_series(symbols: list[str], start: str, end: str) -> pd.DataFrame:
    import FinanceDataReader as fdr

    last_error: Exception | None = None
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    for symbol in symbols:
        try:
            data = fdr.DataReader(symbol, start, end)
            if isinstance(data, pd.DataFrame) and not data.empty:
                data = data.copy()
                data.index = pd.to_datetime(data.index, errors="coerce")
                data = data[(data.index >= start_ts) & (data.index <= end_ts)]
                return data.copy()
        except Exception as exc:
            last_error = exc
            time.sleep(1.0)
    if last_error:
        raise RuntimeError(last_error)
    return pd.DataFrame()


def _append_global_series(frame: pd.DataFrame, spec: dict, data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return frame

    close_col = spec["close_col"]
    change_col = spec["change_col"]
    value_col = None
    for candidate in ["Close", "Adj Close", "close", "adj close"]:
        if candidate in data.columns:
            value_col = candidate
            break
    if value_col is None:
        numeric_cols = [col for col in data.columns if pd.api.types.is_numeric_dtype(data[col])]
        if not numeric_cols:
            raise ValueError(f"no numeric value column found: {list(data.columns)}")
        value_col = numeric_cols[0]

    series = pd.DataFrame(
        {
            "date": pd.to_datetime(data.index),
            close_col: pd.to_numeric(data[value_col], errors="coerce"),
        }
    )
    series[change_col] = series[close_col].pct_change() * 100

    merged = frame.merge(series, on="date", how="outer", suffixes=("", "_new"))
    for col in [close_col, change_col]:
        new_col = f"{col}_new"
        if new_col in merged.columns:
            if col not in merged.columns:
                merged[col] = np.nan
            merged[col] = merged[col].where(merged[col].notna(), merged[new_col])
            merged[col] = merged[new_col].where(merged[new_col].notna(), merged[col])
            merged = merged.drop(columns=[new_col])
    return merged


def check_status() -> None:
    """Print a concise status summary for OHLCV and global data."""
    print("=" * 60)
    print("  FDR Data Status")
    print("=" * 60)

    csv_files = sorted(OHLCV_DIR.glob("*.csv"))
    print(f"OHLCV files: {len(csv_files)}")
    small_files = [path for path in csv_files if path.stat().st_size < 1024]
    if small_files:
        print(f"Small OHLCV files (<1KB): {len(small_files)}")
        for path in small_files[:10]:
            print(f"  - {path.name} ({path.stat().st_size} bytes)")

    sample_path = OHLCV_DIR / "005930.csv"
    if sample_path.exists():
        sample = pd.read_csv(sample_path)
        sample.columns = [str(col).strip().lower() for col in sample.columns]
        sample["date"] = pd.to_datetime(sample["date"], errors="coerce")
        sample = sample.dropna(subset=["date"])
        if not sample.empty:
            print(
                "Samsung sample: "
                f"{sample['date'].min().strftime('%Y-%m-%d')} -> "
                f"{sample['date'].max().strftime('%Y-%m-%d')} "
                f"({len(sample)} rows)"
            )

    if GLOBAL_CSV.exists():
        global_df = _load_global_merged()
        if not global_df.empty:
            print(
                "Global merged: "
                f"{global_df['date'].min().strftime('%Y-%m-%d')} -> "
                f"{global_df['date'].max().strftime('%Y-%m-%d')} "
                f"({len(global_df)} rows)"
            )
            cols = [col for col in global_df.columns if col != "date"]
            print(f"Global columns: {', '.join(cols)}")
    else:
        print("global_merged.csv: missing")

    print("=" * 60)


def update_global() -> bool:
    """Refresh merged global market series."""
    logger.info("Refreshing global_merged.csv")
    GLOBAL_DIR.mkdir(parents=True, exist_ok=True)

    frame = _load_global_merged()
    if frame.empty:
        frame = pd.DataFrame(columns=["date"])

    end = datetime.now().strftime("%Y-%m-%d")
    updated_any = False

    for name, spec in GLOBAL_SPECS.items():
        close_col = spec["close_col"]
        if close_col in frame.columns and frame[close_col].notna().any():
            start = (frame.loc[frame[close_col].notna(), "date"].max() + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start = GLOBAL_START_DATE

        if pd.Timestamp(start) > pd.Timestamp(end):
            logger.info("%s: already up to date", name)
            continue

        try:
            data = _fetch_series(spec["symbols"], start, end)
            if data.empty:
                logger.info("%s: no new rows", name)
                continue
            frame = _append_global_series(frame, spec, data)
            logger.info("%s: appended %d rows from %s", name, len(data), start)
            updated_any = True
            time.sleep(0.25)
        except Exception as exc:
            logger.warning("%s refresh failed: %s", name, exc)

    if not frame.empty:
        frame = frame.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
        frame = frame[frame["date"] >= pd.Timestamp(GLOBAL_START_DATE)].reset_index(drop=True)
        for column in frame.columns:
            if column == "date":
                continue
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
        frame.to_csv(GLOBAL_CSV, index=False, encoding="utf-8-sig")
        logger.info("global_merged.csv saved: %d rows", len(frame))

    return updated_any or GLOBAL_CSV.exists()


def update_ohlcv_single(code: str) -> int:
    """Refresh a single OHLCV CSV incrementally."""
    import FinanceDataReader as fdr

    code = str(code).strip().zfill(6)
    path = OHLCV_DIR / f"{code}.csv"

    if path.exists():
        current = pd.read_csv(path)
        current.columns = [str(col).strip().lower() for col in current.columns]
        current["date"] = pd.to_datetime(current["date"], errors="coerce")
        current = current.dropna(subset=["date"])
        start = (current["date"].max() + timedelta(days=1)).strftime("%Y-%m-%d") if not current.empty else GLOBAL_START_DATE
    else:
        current = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        start = GLOBAL_START_DATE

    end = datetime.now().strftime("%Y-%m-%d")
    if pd.Timestamp(start) > pd.Timestamp(end):
        return 0
    try:
        fresh = fdr.DataReader(code, start, end)
        if fresh is None or fresh.empty:
            return 0
    except Exception:
        return -1

    incoming = pd.DataFrame(
        {
            "date": pd.to_datetime(fresh.index),
            "open": pd.to_numeric(fresh["Open"], errors="coerce"),
            "high": pd.to_numeric(fresh["High"], errors="coerce"),
            "low": pd.to_numeric(fresh["Low"], errors="coerce"),
            "close": pd.to_numeric(fresh["Close"], errors="coerce"),
            "volume": pd.to_numeric(fresh["Volume"], errors="coerce"),
        }
    )
    incoming = incoming.dropna(subset=["date"]).sort_values("date")

    if not current.empty:
        combined = pd.concat([current, incoming], ignore_index=True)
        combined = combined.drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)
        added = max(0, len(combined) - len(current))
    else:
        combined = incoming.reset_index(drop=True)
        added = len(combined)

    if added == 0:
        return 0

    OHLCV_DIR.mkdir(parents=True, exist_ok=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.strftime("%Y-%m-%d")
    combined.to_csv(path, index=False, encoding="utf-8-sig")
    return added


def update_ohlcv_all(full: bool = False) -> None:
    """Refresh all OHLCV files with smart skip."""
    if MAPPING_CSV.exists():
        mapping = pd.read_csv(MAPPING_CSV, dtype={"code": str}, encoding="utf-8-sig")
        mapping["code"] = mapping["code"].astype(str).str.zfill(6)
        codes = mapping["code"].tolist()
    else:
        codes = [path.stem for path in OHLCV_DIR.glob("*.csv")]

    logger.info("Refreshing OHLCV universe: %d codes", len(codes))
    updated = 0
    skipped = 0
    failed = 0

    latest_trading_day = None
    try:
        update_ohlcv_single("005930")
        sample = pd.read_csv(OHLCV_DIR / "005930.csv")
        sample.columns = [str(col).strip().lower() for col in sample.columns]
        sample["date"] = pd.to_datetime(sample["date"], errors="coerce")
        latest_trading_day = sample["date"].max().strftime("%Y-%m-%d")
        logger.info("Latest trading day inferred from 005930: %s", latest_trading_day)
    except Exception:
        logger.warning("Could not infer latest trading day from 005930")

    for index, code in enumerate(codes, start=1):
        if code == "005930":
            skipped += 1
            continue

        csv_path = OHLCV_DIR / f"{code}.csv"
        if latest_trading_day and not full and csv_path.exists():
            try:
                tail = pd.read_csv(csv_path, usecols=["date"]).iloc[-1]["date"]
                if str(tail)[:10] >= latest_trading_day:
                    skipped += 1
                    continue
            except Exception:
                pass

        result = update_ohlcv_single(code)
        if result > 0:
            updated += 1
        elif result == 0:
            skipped += 1
        else:
            failed += 1

        if index % 200 == 0 or index == len(codes):
            logger.info(
                "OHLCV progress: %d/%d updated=%d skipped=%d failed=%d",
                index,
                len(codes),
                updated,
                skipped,
                failed,
            )
        time.sleep(0.15)

    logger.info("OHLCV refresh done: updated=%d skipped=%d failed=%d", updated, skipped, failed)


def main() -> None:
    parser = argparse.ArgumentParser(description="ClosingBell FDR data refresh")
    parser.add_argument("--check", action="store_true", help="Print current status only")
    parser.add_argument("--global-only", action="store_true", help="Refresh global_merged.csv only")
    parser.add_argument("--code", type=str, default="", help="Refresh one stock code only")
    parser.add_argument("--full", action="store_true", help="Force OHLCV update without smart skip")
    args = parser.parse_args()

    if args.check:
        check_status()
        return

    if args.global_only:
        update_global()
        return

    if args.code:
        result = update_ohlcv_single(args.code)
        if result > 0:
            logger.info("%s: added %d rows", args.code, result)
        elif result == 0:
            logger.info("%s: already up to date", args.code)
        else:
            logger.error("%s: refresh failed", args.code)
        return

    update_global()
    update_ohlcv_all(full=args.full)


if __name__ == "__main__":
    main()
