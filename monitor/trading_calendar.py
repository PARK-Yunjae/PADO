"""
KRX trading-day utilities.

Historical counts use actual OHLCV trading sessions from a reference symbol.
Forward projections beyond the last known session use a maintained holiday file.
"""

from __future__ import annotations

import json
from bisect import bisect_right
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path

import pandas as pd

from config import KRX_HOLIDAYS_PATH, OHLCV_DIR, TRADING_CALENDAR_REFERENCE_CODE


def _coerce_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime().date()
    text = str(value).strip()
    if not text:
        return None
    return datetime.strptime(text[:10], "%Y-%m-%d").date()


def _reference_csv_path() -> Path | None:
    preferred = OHLCV_DIR / f"{TRADING_CALENDAR_REFERENCE_CODE}.csv"
    if preferred.exists():
        return preferred
    for path in sorted(OHLCV_DIR.glob("*.csv")):
        if not path.stem.startswith("INDEX_"):
            return path
    return None


@lru_cache(maxsize=1)
def known_sessions() -> tuple[date, ...]:
    path = _reference_csv_path()
    if path is None:
        return ()
    try:
        df = pd.read_csv(path, usecols=["date"])
    except ValueError:
        df = pd.read_csv(path)
    except Exception:
        return ()
    if "date" not in df.columns:
        return ()
    dates = pd.to_datetime(df["date"], errors="coerce").dropna()
    return tuple(sorted({ts.date() for ts in dates}))


@lru_cache(maxsize=1)
def known_session_set() -> set[date]:
    return set(known_sessions())


@lru_cache(maxsize=1)
def holiday_set() -> set[date]:
    if not KRX_HOLIDAYS_PATH.exists():
        return set()
    try:
        raw = json.loads(KRX_HOLIDAYS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return set()
    items = raw.get("holidays", raw) if isinstance(raw, dict) else raw
    holidays: set[date] = set()
    for item in items:
        value = item.get("date") if isinstance(item, dict) else item
        day = _coerce_date(value)
        if day is not None:
            holidays.add(day)
    return holidays


def last_known_session() -> date | None:
    sessions = known_sessions()
    return sessions[-1] if sessions else None


def is_projected_trading_day(value) -> bool:
    day = _coerce_date(value)
    if day is None:
        return False
    return day.weekday() < 5 and day not in holiday_set()


def is_trading_day(value) -> bool:
    day = _coerce_date(value)
    if day is None:
        return False
    sessions = known_sessions()
    if sessions and day <= sessions[-1]:
        return day in known_session_set()
    return is_projected_trading_day(day)


def trading_days_between(start, end) -> int:
    start_day = _coerce_date(start)
    end_day = _coerce_date(end)
    if start_day is None or end_day is None or end_day <= start_day:
        return 0

    sessions = known_sessions()
    if sessions:
        last_known = sessions[-1]
        if end_day <= last_known:
            return max(
                0,
                bisect_right(sessions, end_day) - bisect_right(sessions, start_day),
            )

        past_count = 0
        projection_start = start_day
        if start_day < last_known:
            past_count = len(sessions) - bisect_right(sessions, start_day)
            projection_start = last_known

        future_count = 0
        current = projection_start + timedelta(days=1)
        while current <= end_day:
            if is_projected_trading_day(current):
                future_count += 1
            current += timedelta(days=1)
        return past_count + future_count

    count = 0
    current = start_day + timedelta(days=1)
    while current <= end_day:
        if is_projected_trading_day(current):
            count += 1
        current += timedelta(days=1)
    return count


def trading_days_since(start, asof=None) -> int:
    """
    start 이후 경과 거래일 수.
    asof 미지정 시 현재 시각 기준:
    - 장 마감(15:30) 후 → 오늘 포함
    - 장 개장 전(09:00 이전) → 오늘 미포함 (아직 거래 안 됨)
    """
    if asof is not None:
        end_day = _coerce_date(asof)
    else:
        now = datetime.now()
        end_day = now.date()
        # 장 개장 전이면 오늘을 거래일로 카운트하지 않음
        if now.hour < 9 and is_projected_trading_day(end_day):
            # 직전 거래일까지만 카운트
            d = end_day - timedelta(days=1)
            while d > (end_day - timedelta(days=7)):
                if is_projected_trading_day(d) or d in known_session_set():
                    end_day = d
                    break
                d -= timedelta(days=1)
    return trading_days_between(start, end_day)


def add_trading_days(start, n: int) -> str:
    start_day = _coerce_date(start)
    if start_day is None:
        raise ValueError("invalid start date")
    if n <= 0:
        return start_day.isoformat()

    sessions = known_sessions()
    current = start_day
    remaining = n

    if sessions:
        last_known = sessions[-1]
        if start_day < last_known:
            next_idx = bisect_right(sessions, start_day)
            known_remaining = len(sessions) - next_idx
            if remaining <= known_remaining:
                return sessions[next_idx + remaining - 1].isoformat()
            remaining -= known_remaining
            current = last_known

    while remaining > 0:
        current += timedelta(days=1)
        if is_projected_trading_day(current):
            remaining -= 1
    return current.isoformat()
