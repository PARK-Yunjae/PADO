"""
Performance tracking utilities for screening picks and saved daily buy picks.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime

import pandas as pd

from config import (
    API_DELAY,
    KIWOOM_APPKEY,
    KIWOOM_BASE_URL,
    KIWOOM_SECRETKEY,
    OHLCV_DIR,
    PERFORMANCE_DIR,
    PERFORMANCE_TRACK_DAYS,
)
from core.storage import (
    get_buy_picks,
    iter_screen_results,
    list_buy_pick_dates,
    save_buy_pick_outcomes,
    update_pick_snapshot_returns,
)
from monitor.trading_calendar import trading_days_between

logger = logging.getLogger("closingbell")
PERF_FILE = PERFORMANCE_DIR / "tracking.json"


def _load_tracking() -> dict:
    if PERF_FILE.exists():
        return json.loads(PERF_FILE.read_text(encoding="utf-8"))
    return {"records": [], "last_updated": None}


def _save_tracking(data: dict) -> None:
    data["last_updated"] = datetime.now().isoformat(timespec="seconds")
    PERF_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _iter_screen_logs() -> list[dict]:
    return iter_screen_results()


def _load_ohlcv(code: str) -> pd.DataFrame:
    csv_path = OHLCV_DIR / f"{str(code).strip().zfill(6)}.csv"
    if not csv_path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        return pd.DataFrame()
    df.columns = [c.lower() for c in df.columns]
    if "date" not in df.columns:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.sort_values("date")


def track_today() -> int:
    """
    Update screening performance for the current trading day using live API prices.
    """
    from core.kiwoom_api import KiwoomAPI

    today = datetime.now().strftime("%Y-%m-%d")
    tracking = _load_tracking()
    existing_keys = {
        f"{row['rec_date']}_{row['code']}_{row['track_day']}"
        for row in tracking["records"]
    }

    api = KiwoomAPI(KIWOOM_APPKEY, KIWOOM_SECRETKEY, KIWOOM_BASE_URL, API_DELAY)
    api.ensure_token()

    new_records = 0

    for data in _iter_screen_logs():
        rec_date = data.get("date", "")
        if not rec_date or data.get("skipped") or rec_date >= today:
            continue

        top = data.get("top", [])
        if not top:
            continue

        days_diff = trading_days_between(rec_date, today)
        if days_diff < 1 or days_diff > PERFORMANCE_TRACK_DAYS:
            continue

        for stock in top:
            key = f"{rec_date}_{stock['code']}_{days_diff}"
            if key in existing_keys:
                continue

            try:
                cur = api.get_current_price(stock["code"])
                if cur["price"] <= 0:
                    continue
                buy_price = stock.get("price", 0)
                if not buy_price or buy_price <= 0:
                    continue
            except Exception as exc:
                logger.debug(
                    "Screen tracking failed [%s %s]: %s",
                    rec_date,
                    stock.get("code", ""),
                    exc,
                )
                continue

            ret = (cur["price"] / buy_price - 1) * 100
            record = {
                "rec_date": rec_date,
                "code": stock["code"],
                "name": stock.get("name", ""),
                "rank": stock.get("rank", 0),
                "score": stock.get("score", 0),
                "buy_price": buy_price,
                "track_day": days_diff,
                "track_date": today,
                "track_price": cur["price"],
                "return_pct": round(ret, 2),
                "win": ret > 0,
            }
            tracking["records"].append(record)
            existing_keys.add(key)
            new_records += 1

    _save_tracking(tracking)
    live_rows = track_buy_picks_from_ohlcv()
    logger.info(
        "Screen tracking updated: %d new rows, live pick outcomes refreshed: %d rows",
        new_records,
        live_rows,
    )
    return new_records


def track_from_ohlcv() -> int:
    """
    Rebuild screening performance from OHLCV data and refresh saved buy-pick outcomes.
    """
    tracking = {"records": [], "last_updated": None}

    for data in _iter_screen_logs():
        rec_date = data.get("date", "")
        if not rec_date or data.get("skipped"):
            continue

        for stock in data.get("top", []):
            buy_price = stock.get("price", 0)
            if not buy_price or buy_price <= 0:
                continue

            df = _load_ohlcv(stock.get("code", ""))
            if df.empty:
                continue

            future_days = df[df["date"] > pd.Timestamp(rec_date)].head(
                PERFORMANCE_TRACK_DAYS
            )
            for day_no, (_, row) in enumerate(future_days.iterrows(), start=1):
                ret = (row["close"] / buy_price - 1) * 100
                tracking["records"].append(
                    {
                        "rec_date": rec_date,
                        "code": str(stock.get("code", "")).strip().zfill(6),
                        "name": stock.get("name", ""),
                        "rank": stock.get("rank", 0),
                        "score": stock.get("score", 0),
                        "buy_price": buy_price,
                        "track_day": day_no,
                        "track_date": row["date"].strftime("%Y-%m-%d"),
                        "track_price": int(row["close"]),
                        "return_pct": round(ret, 2),
                        "win": ret > 0,
                    }
                )

    _save_tracking(tracking)
    live_rows = track_buy_picks_from_ohlcv()
    logger.info(
        "OHLCV rebuild complete: screen rows=%d, live pick rows=%d",
        len(tracking["records"]),
        live_rows,
    )
    return len(tracking["records"])


def track_buy_picks_from_ohlcv() -> int:
    """
    Compute D+1~D+5 outcomes for saved 15:00 buy picks and store them in SQLite.
    """
    records = []

    for pick_date in list_buy_pick_dates(desc=False):
        payload = get_buy_picks(pick_date) or {}
        picks = payload.get("picks", [])
        if not picks:
            continue

        for order, pick in enumerate(picks, start=1):
            code = str(pick.get("code", "")).strip().zfill(6)
            buy_price = pick.get("current_price", pick.get("buy_price", 0))
            if not code or not buy_price:
                continue

            df = _load_ohlcv(code)
            if df.empty:
                continue

            try:
                base_price = float(buy_price)
            except (TypeError, ValueError):
                continue
            if base_price <= 0:
                continue

            future_days = df[df["date"] > pd.Timestamp(pick_date)].head(
                PERFORMANCE_TRACK_DAYS
            )
            for day_no, (_, row) in enumerate(future_days.iterrows(), start=1):
                try:
                    open_price = float(row.get("open", 0) or 0)
                    high_price = float(row.get("high", 0) or 0)
                    low_price = float(row.get("low", 0) or 0)
                    close_price = float(row.get("close", 0) or 0)
                except (TypeError, ValueError):
                    continue
                if close_price <= 0:
                    continue

                open_ret = (open_price / base_price - 1) * 100 if open_price > 0 else None
                high_ret = (high_price / base_price - 1) * 100 if high_price > 0 else None
                low_ret = (low_price / base_price - 1) * 100 if low_price > 0 else None
                close_ret = (close_price / base_price - 1) * 100

                records.append(
                    {
                        "pick_date": pick_date,
                        "signal_date": pick_date,
                        "code": code,
                        "name": pick.get("name", code),
                        "pick_order": order,
                        "rank": pick.get("rank", order),
                        "conviction": pick.get("conviction", ""),
                        "conviction_score": pick.get("conviction_score", 0),
                        "signal_type": pick.get("signal_type", ""),
                        "dart_risk": pick.get("dart_risk", ""),
                        "news_risk": pick.get("news_risk", ""),
                        "ai_action": pick.get("ai_action", ""),
                        "watchlist_date": pick.get("watchlist_date", ""),
                        "days_elapsed": pick.get("days_elapsed", 0),
                        "risk_flags": pick.get("risk_flags", []),
                        "buy_price": int(round(base_price)),
                        "track_day": day_no,
                        "track_date": row["date"].strftime("%Y-%m-%d"),
                        "open_price": int(round(open_price)) if open_price > 0 else 0,
                        "high_price": int(round(high_price)) if high_price > 0 else 0,
                        "low_price": int(round(low_price)) if low_price > 0 else 0,
                        "track_price": int(round(close_price)),
                        "return_pct": round(close_ret, 2),
                        "open_ret": round(open_ret, 2) if open_ret is not None else None,
                        "high_ret": round(high_ret, 2) if high_ret is not None else None,
                        "low_ret": round(low_ret, 2) if low_ret is not None else None,
                        "win": close_ret > 0,
                        "win_open": open_ret > 0 if open_ret is not None else False,
                    }
                )

    saved = save_buy_pick_outcomes(records)
    update_pick_snapshot_returns(records)
    return saved


def generate_report() -> dict:
    tracking = _load_tracking()
    records = tracking.get("records", [])
    if not records:
        return {"error": "No screening performance data available"}

    df = pd.DataFrame(records)
    report = {
        "total_records": len(df),
        "unique_recommendations": int(df["rec_date"].nunique()),
        "period": f"{df['rec_date'].min()} ~ {df['rec_date'].max()}",
    }

    rank_day_stats = []
    for rank in sorted(df["rank"].dropna().unique()):
        for day in range(1, PERFORMANCE_TRACK_DAYS + 1):
            subset = df[(df["rank"] == rank) & (df["track_day"] == day)]
            if subset.empty:
                continue
            wins = int(subset["win"].sum())
            total = int(len(subset))
            rank_day_stats.append(
                {
                    "rank": int(rank),
                    "track_day": f"D+{day}",
                    "trades": total,
                    "wins": wins,
                    "win_rate": round(wins / total * 100, 1),
                    "avg_return": round(float(subset["return_pct"].mean()), 2),
                }
            )
    report["rank_day_matrix"] = rank_day_stats

    day_stats = []
    for day in range(1, PERFORMANCE_TRACK_DAYS + 1):
        subset = df[df["track_day"] == day]
        if subset.empty:
            continue
        wins = int(subset["win"].sum())
        total = int(len(subset))
        day_stats.append(
            {
                "track_day": f"D+{day}",
                "trades": total,
                "win_rate": round(wins / total * 100, 1),
                "avg_return": round(float(subset["return_pct"].mean()), 2),
                "median_return": round(float(subset["return_pct"].median()), 2),
            }
        )
    report["day_stats"] = day_stats

    best = df.loc[df["return_pct"].idxmax()]
    worst = df.loc[df["return_pct"].idxmin()]
    report["best"] = {
        "name": best["name"],
        "rec_date": best["rec_date"],
        "track_day": f"D+{best['track_day']}",
        "return_pct": best["return_pct"],
    }
    report["worst"] = {
        "name": worst["name"],
        "rec_date": worst["rec_date"],
        "track_day": f"D+{worst['track_day']}",
        "return_pct": worst["return_pct"],
    }
    return report




if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="ClosingBell performance tracking")
    parser.add_argument("--report", action="store_true", help="Print screening report")
    parser.add_argument("--rebuild", action="store_true", help="Rebuild from OHLCV")
    args = parser.parse_args()

    if args.report:
        print(json.dumps(generate_report(), ensure_ascii=False, indent=2))
    elif args.rebuild:
        track_from_ohlcv()
        print(json.dumps(generate_report(), ensure_ascii=False, indent=2))
    else:
        track_today()



