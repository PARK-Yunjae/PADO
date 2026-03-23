#!/usr/bin/env python3
"""
generate_universe.py — stock_mapping.csv 기반 유니버스 화이트리스트 생성
=====================================================================
FDR 불필요. C:/Coding/data/stock_mapping.csv만 있으면 됨.

코넥스/스팩/리츠를 제거하고 코스피+코스닥(+KOSDAQ GLOBAL) 유니버스 생성.
우선주는 포함하되 is_preferred 플래그로 태깅 → 분석 시 분리 가능.

사용법:
  python generate_universe.py                         # 기본 실행
  python generate_universe.py --mapping C:/path/to/stock_mapping.csv
  python generate_universe.py --check 244880          # 종목 확인
  python generate_universe.py --stats                 # 통계만 출력

출력: universe_whitelist.csv (같은 폴더에 생성)
"""

import os
import re
import sys
import argparse
import pandas as pd
from datetime import datetime


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_MAPPING = "C:/Coding/data/stock_mapping.csv"


def classify_stock(code: str, name: str) -> str:
    """
    종목 유형 분류.
    Returns: 'common' | 'preferred' | 'spac' | 'reit' | 'etf' | 'other'
    """
    code = str(code).zfill(6)
    name = str(name) if name else ""

    # 우선주
    if re.search(r'우[BCbc]?$|\(우\)$', name):
        return 'preferred'

    # 스팩
    if '스팩' in name or 'SPAC' in name.upper():
        return 'spac'

    # 리츠/인프라
    if '리츠' in name or '인프라' in name or '선박투자' in name:
        return 'reit'

    # ETF (stock_mapping에는 거의 없지만 안전장치)
    etf_kw = ['KODEX', 'TIGER', 'KBSTAR', 'ARIRANG', 'SOL', 'HANARO',
              'KOSEF', 'ACE', 'PLUS', 'RISE', '인버스', '레버리지']
    if any(kw in name for kw in etf_kw):
        return 'etf'

    return 'common'


def generate_universe(mapping_csv: str = DEFAULT_MAPPING,
                      output_path: str = None) -> pd.DataFrame:
    """유니버스 생성 메인 함수"""

    if output_path is None:
        output_path = os.path.join(SCRIPT_DIR, "universe_whitelist.csv")

    print("=" * 60)
    print("PADO 유니버스 화이트리스트 생성기")
    print(f"소스: {mapping_csv}")
    print("=" * 60)

    # 로드
    df = pd.read_csv(mapping_csv, dtype={'code': str}, encoding='utf-8-sig')
    df['code'] = df['code'].str.zfill(6)
    total = len(df)
    print(f"\n전체 종목: {total}")

    # 시장별 분포
    print(f"\n[시장 분포]")
    for mkt, cnt in df['market'].value_counts().items():
        print(f"  {mkt}: {cnt}")

    # ── 1. 코넥스 제거 ──
    konex = df[df['market'] == 'KONEX']
    df = df[df['market'] != 'KONEX']
    print(f"\n[1] 코넥스 제거: {len(konex)}종목 -> 남은 {len(df)}")

    # ── 2. 종목 유형 분류 ──
    df['stock_type'] = df.apply(lambda r: classify_stock(r['code'], r['name']), axis=1)

    type_counts = df['stock_type'].value_counts()
    print(f"\n[2] 종목 유형 분류:")
    for t, c in type_counts.items():
        print(f"  {t}: {c}")

    # ── 3. 스팩/리츠/ETF 제거 (우선주는 유지) ──
    remove_types = {'spac', 'reit', 'etf'}
    removed = df[df['stock_type'].isin(remove_types)]
    df = df[~df['stock_type'].isin(remove_types)]
    print(f"\n[3] 스팩/리츠/ETF 제거: {len(removed)}종목 -> 남은 {len(df)}")

    for t in remove_types:
        t_df = removed[removed['stock_type'] == t]
        if len(t_df) > 0:
            print(f"  {t}: {len(t_df)}종목")

    # ── 4. 플래그 추가 ──
    df['is_preferred'] = (df['stock_type'] == 'preferred').astype(int)
    df['is_common'] = (df['stock_type'] == 'common').astype(int)

    # ── 5. 저장 ──
    out_cols = ['code', 'name', 'market', 'sector', 'stock_type',
                'is_preferred', 'is_common']
    available = [c for c in out_cols if c in df.columns]
    result = df[available].sort_values('code').reset_index(drop=True)
    result.to_csv(output_path, index=False, encoding='utf-8-sig')

    # 메타
    meta_path = output_path.replace('.csv', '_meta.txt')
    common = result[result['is_common'] == 1]
    preferred = result[result['is_preferred'] == 1]

    with open(meta_path, 'w', encoding='utf-8') as f:
        f.write(f"생성일: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"소스: {mapping_csv}\n")
        f.write(f"원본 종목수: {total}\n")
        f.write(f"최종 종목수: {len(result)}\n")
        f.write(f"  보통주: {len(common)}\n")
        f.write(f"  우선주: {len(preferred)} (포함, 분리 분석용)\n")
        f.write(f"  KOSPI: {len(result[result['market']=='KOSPI'])}\n")
        f.write(f"  KOSDAQ: {len(result[result['market']=='KOSDAQ'])}\n")
        f.write(f"  KOSDAQ GLOBAL: {len(result[result['market']=='KOSDAQ GLOBAL'])}\n")
        f.write(f"제외: 코넥스 {len(konex)}, 스팩/리츠/ETF {len(removed)}\n")

    # ── 결과 요약 ──
    print("\n" + "=" * 60)
    print("완료!")
    print("=" * 60)
    print(f"  원본: {total}종목")
    print(f"  최종: {len(result)}종목 (보통주 {len(common)} + 우선주 {len(preferred)})")
    print(f"  KOSPI: {len(result[result['market']=='KOSPI'])}")
    print(f"  KOSDAQ: {len(result[result['market']=='KOSDAQ'])}")
    print(f"  KOSDAQ GLOBAL: {len(result[result['market']=='KOSDAQ GLOBAL'])}")
    print(f"\n  -> {output_path}")
    print(f"  -> {meta_path}")

    return result


def check_code(code: str, mapping_csv: str = DEFAULT_MAPPING):
    """특정 종목코드 확인"""
    df = pd.read_csv(mapping_csv, dtype={'code': str}, encoding='utf-8-sig')
    df['code'] = df['code'].str.zfill(6)
    code = str(code).zfill(6)

    match = df[df['code'] == code]
    if len(match) == 0:
        print(f"  {code} -- stock_mapping에 없음")
        return

    r = match.iloc[0]
    stype = classify_stock(r['code'], r['name'])
    excluded = r['market'] == 'KONEX' or stype in ('spac', 'reit', 'etf')

    icon = "[제외]" if excluded else "[포함]"
    tag = f" (우선주)" if stype == 'preferred' else ""
    print(f"{icon} {r['code']} {r['name']}{tag}")
    print(f"  시장: {r['market']}, 유형: {stype}, 섹터: {r.get('sector','')}")


def show_stats(mapping_csv: str = DEFAULT_MAPPING):
    """통계만 출력"""
    df = pd.read_csv(mapping_csv, dtype={'code': str}, encoding='utf-8-sig')
    df['code'] = df['code'].str.zfill(6)
    df['stock_type'] = df.apply(lambda r: classify_stock(r['code'], r['name']), axis=1)

    print(f"stock_mapping.csv 분석 ({len(df)}종목)")
    print("-" * 50)
    for mkt in sorted(df['market'].unique()):
        mdf = df[df['market'] == mkt]
        types = mdf['stock_type'].value_counts()
        detail = ", ".join(f"{t}:{c}" for t, c in types.items())
        print(f"  {mkt:<15} {len(mdf):>5}종목  ({detail})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='PADO 유니버스 화이트리스트 생성')
    parser.add_argument('--mapping', '-m', default=DEFAULT_MAPPING,
                        help='stock_mapping.csv 경로')
    parser.add_argument('--output', '-o', default=None,
                        help='출력 경로 (기본: 스크립트 폴더/universe_whitelist.csv)')
    parser.add_argument('--check', '-c', type=str, default=None,
                        help='특정 종목코드 확인')
    parser.add_argument('--stats', action='store_true',
                        help='통계만 출력')
    args = parser.parse_args()

    if args.stats:
        show_stats(args.mapping)
    elif args.check:
        check_code(args.check, args.mapping)
    else:
        generate_universe(args.mapping, args.output)
