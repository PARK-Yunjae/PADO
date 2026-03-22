"""
Weekly and periodic data maintenance for ClosingBell.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from config import (
    COMPANY_PROFILE_CSV,
    DART_API_KEY,
    DATA_DIR,
    MAJOR_HOLDER_CSV,
    MAPPING_CSV,
    PROJECT_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("weekly")

META_DIR = DATA_DIR / "meta"
FINSTATE_DIR = DATA_DIR / "finstate"
DART_BASE = "https://opendart.fss.or.kr/api"
CORP_MAP_CACHE = PROJECT_DIR / "data" / "dart_corp_map.json"

REPRT_ANNUAL = "11011"
REPRT_Q3 = "11014"
REPRT_SEMI = "11012"
REPRT_Q1 = "11013"


def _normalize_code(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.zfill(6)


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _status_label(days_old: int, warn_days: int, stale_days: int) -> str:
    if days_old <= warn_days:
        return "OK"
    if days_old <= stale_days:
        return "WARN"
    return "STALE"


def _save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _fetch_stock_listing(name: str) -> pd.DataFrame:
    import FinanceDataReader as fdr

    last_error: Exception | None = None
    for _ in range(3):
        try:
            df = fdr.StockListing(name)
            if isinstance(df, pd.DataFrame) and not df.empty:
                return df.copy()
        except Exception as exc:
            last_error = exc
            time.sleep(1.0)
    raise RuntimeError(f"StockListing failed for {name}: {last_error}")


def _rename_listing_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.copy()
    renamed.columns = [str(col).strip() for col in renamed.columns]
    col_map = {
        "Code": "code",
        "code": "code",
        "Symbol": "code",
        "Name": "name",
        "name": "name",
        "Market": "market",
        "market": "market",
        "Sector": "sector",
        "sector": "sector",
        "Industry": "industry",
        "industry": "industry",
        "ListingDate": "listing_date",
        "Region": "region",
        "Representative": "representative",
        "HomePage": "homepage",
        "SettleMonth": "settle_month",
    }
    renamed = renamed.rename(columns={key: value for key, value in col_map.items() if key in renamed.columns})
    if "code" not in renamed.columns:
        raise ValueError(f"missing code column: {list(df.columns)}")
    renamed["code"] = _normalize_code(renamed["code"])
    return renamed


def _merge_mapping_frames(base: pd.DataFrame, detail: pd.DataFrame | None) -> pd.DataFrame:
    base = _rename_listing_columns(base)
    if detail is not None and not detail.empty:
        detail = _rename_listing_columns(detail)
        use_cols = [col for col in [
            "code",
            "industry",
            "listing_date",
            "settle_month",
            "representative",
            "homepage",
            "region",
        ] if col in detail.columns]
        merged = base.merge(detail[use_cols], on="code", how="left", suffixes=("", "_detail"))
        if "industry" not in merged.columns and "industry_detail" in merged.columns:
            merged["industry"] = merged["industry_detail"]
        elif "industry_detail" in merged.columns:
            merged["industry"] = merged["industry"].replace("", pd.NA).fillna(merged["industry_detail"])
            merged = merged.drop(columns=["industry_detail"])
    else:
        merged = base

    keep_cols = [col for col in [
        "code",
        "name",
        "market",
        "sector",
        "industry",
        "listing_date",
        "settle_month",
        "representative",
        "homepage",
        "region",
    ] if col in merged.columns]
    merged = merged[keep_cols].drop_duplicates("code").sort_values("code").reset_index(drop=True)
    return merged


def update_stock_mapping() -> bool:
    """Refresh the main code/name/sector mapping."""
    try:
        logger.info("Refreshing stock_mapping.csv")
        detail = None
        base = None
        try:
            kospi = _fetch_stock_listing("KOSPI")
            kosdaq = _fetch_stock_listing("KOSDAQ")
            base = pd.concat([kospi, kosdaq], ignore_index=True)
        except Exception as exc:
            logger.warning("Live KOSPI/KOSDAQ fetch failed: %s", exc)

        try:
            detail = _fetch_stock_listing("KRX-DESC")
        except Exception as exc:
            logger.warning("KRX-DESC fetch failed during mapping refresh: %s", exc)

        if base is None:
            local_desc = META_DIR / "krx_desc.csv"
            if local_desc.exists():
                base = pd.read_csv(local_desc, dtype={"Code": str}, encoding="utf-8-sig")
                logger.info("Using local krx_desc.csv as stock_mapping fallback")
            elif MAPPING_CSV.exists():
                base = pd.read_csv(MAPPING_CSV, dtype={"code": str}, encoding="utf-8-sig")
                logger.info("Using existing stock_mapping.csv as fallback source")
            else:
                raise RuntimeError("no live or local mapping source available")

        mapping = _merge_mapping_frames(base, detail)
        _save_csv(mapping, MAPPING_CSV)
        logger.info("stock_mapping.csv updated: %d rows", len(mapping))
        return True
    except Exception as exc:
        logger.error("stock_mapping refresh failed: %s", exc)
        return False


def build_company_profile() -> bool:
    """Build an enriched company profile file from mapping + KRX detail."""
    try:
        if not MAPPING_CSV.exists():
            logger.warning("company profile skipped: stock_mapping.csv missing")
            return False

        mapping = pd.read_csv(MAPPING_CSV, dtype={"code": str}, encoding="utf-8-sig")
        mapping["code"] = _normalize_code(mapping["code"])

        detail_path = META_DIR / "krx_desc.csv"
        if detail_path.exists():
            detail = _rename_listing_columns(pd.read_csv(detail_path, dtype={"Code": str}, encoding="utf-8-sig"))
            merged = mapping.merge(
                detail[
                    [col for col in [
                        "code",
                        "industry",
                        "listing_date",
                        "settle_month",
                        "representative",
                        "homepage",
                        "region",
                    ] if col in detail.columns]
                ],
                on="code",
                how="left",
                suffixes=("", "_detail"),
            )
            if "industry_detail" in merged.columns:
                merged["industry"] = merged.get("industry", "").replace("", pd.NA).fillna(merged["industry_detail"])
                merged = merged.drop(columns=["industry_detail"])
        else:
            merged = mapping.copy()

        merged["sector"] = merged.get("sector", "").fillna("")
        merged["industry"] = merged.get("industry", "").fillna("")
        merged["company_brief"] = merged.apply(
            lambda row: " | ".join(part for part in [row.get("sector", ""), row.get("industry", "")] if part),
            axis=1,
        )
        merged["main_products"] = merged["industry"]
        merged["updated_at"] = _now_text()
        merged["source"] = "krx_desc"

        keep_cols = [col for col in [
            "code",
            "name",
            "market",
            "sector",
            "industry",
            "company_brief",
            "main_products",
            "listing_date",
            "settle_month",
            "representative",
            "homepage",
            "region",
            "updated_at",
            "source",
        ] if col in merged.columns]
        profile = merged[keep_cols].drop_duplicates("code").sort_values("code").reset_index(drop=True)
        _save_csv(profile, COMPANY_PROFILE_CSV)
        logger.info("company_profile.csv updated: %d rows", len(profile))
        return True
    except Exception as exc:
        logger.error("company profile build failed: %s", exc)
        return False


def update_meta() -> bool:
    """Refresh supporting meta files and rebuild company profile."""
    import FinanceDataReader as fdr

    META_DIR.mkdir(parents=True, exist_ok=True)
    ok = True

    jobs = {
        "krx_desc.csv": "KRX-DESC",
        "admin_list.csv": "KRX-ADMIN",
        "delisting_list.csv": "KRX-DELISTING",
    }

    logger.info("Refreshing meta datasets")
    for filename, market in jobs.items():
        try:
            frame = fdr.StockListing(market)
            _save_csv(frame, META_DIR / filename)
            logger.info("%s updated: %d rows", filename, len(frame))
        except Exception as exc:
            ok = False
            logger.warning("%s refresh failed: %s", filename, exc)

    try:
        kospi = fdr.StockListing("KOSPI")
        kosdaq = fdr.StockListing("KOSDAQ")
        marcap = pd.concat([kospi, kosdaq], ignore_index=True)
        _save_csv(marcap, META_DIR / "marcap_snapshot.csv")
        logger.info("marcap_snapshot.csv updated: %d rows", len(marcap))
    except Exception as exc:
        ok = False
        logger.warning("marcap snapshot refresh failed: %s", exc)

    build_company_profile()
    return ok


def _load_corp_map(force_refresh: bool = False) -> dict[str, str]:
    from checkers.dart_checker import DartChecker

    if not force_refresh and CORP_MAP_CACHE.exists():
        try:
            return json.loads(CORP_MAP_CACHE.read_text(encoding="utf-8"))
        except Exception:
            pass

    checker = DartChecker()
    return dict(getattr(checker, "_corp_map", {}))


def _fetch_major_holder(corp_code: str, bsns_year: int, reprt_code: str) -> dict | None:
    if not DART_API_KEY:
        return None

    try:
        resp = requests.get(
            f"{DART_BASE}/hyslrSttus.json",
            params={
                "crtfc_key": DART_API_KEY,
                "corp_code": corp_code,
                "bsns_year": str(bsns_year),
                "reprt_code": reprt_code,
            },
            timeout=15,
        )
        data = resp.json()
        if data.get("status") == "013":
            return None
        if data.get("status") != "000":
            return None

        holders = data.get("list", [])
        if not holders:
            return None

        total_pct = 0.0
        top_holder = ""
        for holder in holders:
            pct = holder.get("trmend_posesn_stock_qota_rt", "0")
            try:
                total_pct += float(str(pct).replace(",", "").replace("-", "0"))
            except ValueError:
                pass
            if not top_holder and holder.get("relate") in {"본인", "최대주주 본인", ""}:
                top_holder = holder.get("nm", "")

        if not top_holder:
            top_holder = holders[0].get("nm", "")

        return {
            "total_pct": round(total_pct, 2),
            "holder_name": top_holder,
            "holder_count": len(holders),
            "report_code": reprt_code,
        }
    except Exception:
        return None


def update_major_holder(
    years: list[int] | None = None,
    resume: bool = True,
    max_codes: int | None = None,
) -> bool:
    """Refresh major holder ratios from OpenDART."""
    if not DART_API_KEY:
        logger.warning("major holder refresh skipped: DART_API_KEY missing")
        return False
    if not MAPPING_CSV.exists():
        logger.warning("major holder refresh skipped: stock_mapping.csv missing")
        return False

    years = years or [datetime.now().year - 1, datetime.now().year - 2]
    corp_map = _load_corp_map()
    if not corp_map:
        logger.warning("major holder refresh skipped: corp map unavailable")
        return False

    mapping = pd.read_csv(MAPPING_CSV, dtype={"code": str}, encoding="utf-8-sig")
    mapping["code"] = _normalize_code(mapping["code"])
    codes = mapping["code"].tolist()
    if max_codes:
        codes = codes[:max_codes]

    existing = pd.DataFrame()
    if resume and MAJOR_HOLDER_CSV.exists():
        existing = pd.read_csv(MAJOR_HOLDER_CSV, dtype={"code": str}, encoding="utf-8-sig")
        existing["code"] = _normalize_code(existing["code"])

    existing_keys = set()
    if not existing.empty and {"code", "year"}.issubset(existing.columns):
        existing_keys = set(existing["code"] + "_" + existing["year"].astype(str))
    results = existing.to_dict("records") if not existing.empty else []

    success = 0
    skipped = 0
    failed = 0
    for code in codes:
        corp_code = corp_map.get(code)
        if not corp_code:
            failed += 1
            continue
        for year in years:
            key = f"{code}_{year}"
            if key in existing_keys:
                skipped += 1
                continue

            data = _fetch_major_holder(corp_code, year, REPRT_ANNUAL)
            if data is None:
                data = _fetch_major_holder(corp_code, year, REPRT_Q3)

            if data is None:
                failed += 1
                continue

            results.append(
                {
                    "code": code,
                    "year": year,
                    "total_pct": data["total_pct"],
                    "holder_name": data["holder_name"],
                    "holder_count": data["holder_count"],
                    "source": "dart_hyslr",
                    "report_code": data["report_code"],
                    "updated_at": _now_text(),
                }
            )
            success += 1
            time.sleep(0.15)

    if results:
        output = pd.DataFrame(results)
        output["code"] = _normalize_code(output["code"])
        output = output.drop_duplicates(["code", "year"], keep="last").sort_values(["code", "year"])
        _save_csv(output, MAJOR_HOLDER_CSV)

    logger.info(
        "major_holder.csv refreshed: success=%d skipped=%d failed=%d rows=%d",
        success,
        skipped,
        failed,
        len(results),
    )
    return success > 0 or skipped > 0


def _fetch_finstate_accounts(corp_code: str, bsns_year: int, reprt_code: str) -> dict | None:
    if not DART_API_KEY:
        return None

    for fs_div in ("CFS", "OFS"):
        try:
            resp = requests.get(
                f"{DART_BASE}/fnlttSinglAcntAll.json",
                params={
                    "crtfc_key": DART_API_KEY,
                    "corp_code": corp_code,
                    "bsns_year": str(bsns_year),
                    "reprt_code": reprt_code,
                    "fs_div": fs_div,
                },
                timeout=20,
            )
            data = resp.json()
            if data.get("status") == "013":
                continue
            if data.get("status") != "000":
                continue

            rows = data.get("list", [])
            if not rows:
                continue

            accounts: dict[str, dict[str, str]] = {}
            for row in rows:
                account_name = str(row.get("account_nm", "")).strip()
                if not account_name:
                    continue
                accounts[account_name] = {
                    "current": row.get("thstrm_amount", ""),
                    "prior": row.get("frmtrm_amount", ""),
                    "currency": row.get("currency", ""),
                    "account_id": row.get("account_id", ""),
                }

            return {
                "fs_div": fs_div,
                "reprt_code": reprt_code,
                "accounts": accounts,
                "updated_at": _now_text(),
            }
        except Exception:
            continue
    return None


def update_finstate(year: int | None = None, max_codes: int | None = None) -> bool:
    """Refresh basic financial statement snapshots from OpenDART."""
    if not DART_API_KEY:
        logger.warning("finstate refresh skipped: DART_API_KEY missing")
        return False
    if not MAPPING_CSV.exists():
        logger.warning("finstate refresh skipped: stock_mapping.csv missing")
        return False

    bsns_year = int(year or (datetime.now().year - 1))
    corp_map = _load_corp_map()
    if not corp_map:
        logger.warning("finstate refresh skipped: corp map unavailable")
        return False

    FINSTATE_DIR.mkdir(parents=True, exist_ok=True)
    mapping = pd.read_csv(MAPPING_CSV, dtype={"code": str}, encoding="utf-8-sig")
    mapping["code"] = _normalize_code(mapping["code"])
    codes = mapping["code"].tolist()
    if max_codes:
        codes = codes[:max_codes]

    success = 0
    skipped = 0
    failed = 0
    for idx, code in enumerate(codes, start=1):
        corp_code = corp_map.get(code)
        if not corp_code:
            failed += 1
            continue

        path = FINSTATE_DIR / f"{code}.json"
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
                if int(existing.get("year", 0)) == bsns_year:
                    skipped += 1
                    continue
            except Exception:
                pass

        snapshot = _fetch_finstate_accounts(corp_code, bsns_year, REPRT_ANNUAL)
        if snapshot is None:
            snapshot = _fetch_finstate_accounts(corp_code, bsns_year, REPRT_Q3)
        if snapshot is None:
            snapshot = _fetch_finstate_accounts(corp_code, bsns_year, REPRT_SEMI)
        if snapshot is None:
            failed += 1
            continue

        path.write_text(
            json.dumps(
                {
                    "code": code,
                    "year": bsns_year,
                    **snapshot,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        success += 1

        if idx % 100 == 0:
            logger.info(
                "finstate progress: %d/%d success=%d skipped=%d failed=%d",
                idx,
                len(codes),
                success,
                skipped,
                failed,
            )
        time.sleep(0.15)

    logger.info(
        "finstate refresh done: success=%d skipped=%d failed=%d year=%d",
        success,
        skipped,
        failed,
        bsns_year,
    )
    return success > 0


def check_status() -> None:
    """Print a concise data freshness summary."""
    print("=" * 60)
    print("  Weekly Data Status")
    print("=" * 60)

    if MAPPING_CSV.exists():
        mapping = pd.read_csv(MAPPING_CSV, dtype={"code": str}, encoding="utf-8-sig")
        days_old = (datetime.now() - datetime.fromtimestamp(MAPPING_CSV.stat().st_mtime)).days
        status = _status_label(days_old, 7, 30)
        industry_count = int(mapping["industry"].notna().sum()) if "industry" in mapping.columns else 0
        print(f"  [{status}] stock_mapping.csv: {len(mapping)} rows, {days_old}d old, industry={industry_count}")
    else:
        print("  [MISSING] stock_mapping.csv")

    for path in [
        META_DIR / "krx_desc.csv",
        META_DIR / "admin_list.csv",
        META_DIR / "delisting_list.csv",
        COMPANY_PROFILE_CSV,
        MAJOR_HOLDER_CSV,
    ]:
        if path.exists():
            days_old = (datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)).days
            status = _status_label(days_old, 7, 30)
            print(f"  [{status}] {path.name}: {days_old}d old")
        else:
            print(f"  [MISSING] {path.name}")

    if MAJOR_HOLDER_CSV.exists():
        holders = pd.read_csv(MAJOR_HOLDER_CSV, dtype={"code": str}, encoding="utf-8-sig")
        years = sorted(holders["year"].dropna().unique().tolist()) if "year" in holders.columns else []
        print(f"    holder universe: {holders['code'].astype(str).str.zfill(6).nunique()} codes, years={years}")

    if FINSTATE_DIR.exists():
        files = list(FINSTATE_DIR.glob("*"))
        json_count = len(list(FINSTATE_DIR.glob("*.json")))
        csv_count = len(list(FINSTATE_DIR.glob("*.csv")))
        zero_csv = len([path for path in FINSTATE_DIR.glob("*.csv") if path.stat().st_size == 0])
        latest_days = None
        if files:
            latest_mtime = max(path.stat().st_mtime for path in files)
            latest_days = (datetime.now() - datetime.fromtimestamp(latest_mtime)).days
        status = _status_label(latest_days or 999, 90, 180) if files else "WARN"
        print(
            f"  [{status}] finstate/: total={len(files)} json={json_count} csv={csv_count} zero_csv={zero_csv}"
        )
    else:
        print("  [WARN] finstate/: missing")

    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="ClosingBell weekly data maintenance")
    parser.add_argument("--check", action="store_true", help="Print data status only")
    parser.add_argument("--finstate", action="store_true", help="Also refresh finstate snapshots")
    parser.add_argument("--holders", action="store_true", help="Refresh major_holder.csv")
    parser.add_argument("--max-codes", type=int, default=0, help="Limit DART refresh target size")
    args = parser.parse_args()

    if args.check:
        check_status()
        return

    print("=" * 60)
    print("  Weekly Data Refresh")
    print("=" * 60)
    update_stock_mapping()
    update_meta()
    if args.holders:
        update_major_holder(max_codes=args.max_codes or None)
    if args.finstate:
        update_finstate(max_codes=args.max_codes or None)
    check_status()


if __name__ == "__main__":
    main()
