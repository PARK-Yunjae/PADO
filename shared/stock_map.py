"""종목 매핑 로드 (stock_mapping.csv)."""

import csv
from pathlib import Path
from shared.schemas import StockBasic
from config import MAPPING_CSV, setup_logging

logger = setup_logging().getChild("stock_map")

_cache: dict[str, StockBasic] | None = None


def load_stock_map(path: Path | None = None) -> dict[str, StockBasic]:
    global _cache
    if _cache is not None:
        return _cache

    p = path or MAPPING_CSV
    result = {}
    if not p.exists():
        logger.warning(f"stock_mapping.csv 없음: {p}")
        return result

    with open(p, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = str(row.get("code", row.get("종목코드", ""))).strip().zfill(6)
            name = row.get("name", row.get("종목명", "")).strip()
            market = row.get("market", row.get("시장", "")).strip()
            sector = row.get("sector", row.get("섹터", "기타")).strip()
            if code and name:
                # v2: sector → canon 자동 채우기
                themes = []
                try:
                    from shared.theme_taxonomy import normalize_sector
                    canon = normalize_sector(sector)
                    if canon:
                        themes = [canon]
                except Exception:
                    pass

                result[code] = StockBasic(
                    code=code, name=name, market=market, sector=sector,
                    themes=themes,
                )

    _cache = result
    logger.info(f"종목 매핑 로드: {len(result)}종목")
    return result


def get_stock(code: str) -> StockBasic | None:
    m = load_stock_map()
    return m.get(code.zfill(6))


def get_all_codes() -> list[str]:
    m = load_stock_map()
    return list(m.keys())
