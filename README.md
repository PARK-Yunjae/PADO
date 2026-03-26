# PADO v5 — 눌림목 단타 + 재차거시 검증

## 하루 흐름

```
08:20  뉴스 수집 (한글 41 + 영문 47 + 네이버 15 = 123쿼리)
08:25  📰 아침 브리핑 → 디스코드
         해외시황 (나스닥/S&P/VIX)
         외신 주요 뉴스 (Gemini 한글번역)
         뉴스 키워드 + AI 추론
         CB 감시 현황

12:00  📊 장중 시그널 → 디스코드
         코스피/코스닥 등락률
         장중 주도 테마 TOP3
         국내 뉴스 키워드 (한글만)
         CB 감시 현황

15:00  🎯 ClosingBell → 디스코드
         1차: 감시종목 중 눌림목 감지 (유목민 거래량법)
         2차: 잔존률 30%/20%/12% 판정
         3차: 재차거시 검증 (DART+공매도+거래원+뉴스+AI)
         PASS만 매수 추천, 없으면 "오늘 매수 없음"

15:35  📊 장후 파이프라인 (무발송)
         OHLCV 갱신 (2881종목)
         글로벌 지수 갱신
         거래량 터진 종목 → 감시 등록 (점수35+, 최대60개)
         D+5 초과 감시 해제
         재차거시 평가 (60건)
         뉴스 수집 + 분석
```

## 3단계 검증

| 단계 | 역할 | 기준 |
|------|------|------|
| 1차 감시 | 거래량 터진 종목 넓게 | MA20×3배 OR 1000만주, 2000~15만원, ETF/스팩/우선주 제외 |
| 2차 눌림목 | 유목민 거래량법 | D+2 35%이하, D+3 20%이하, 12%미만=극강, D+5해제 |
| 3차 재차거시 | DART+공매도+거래원+뉴스+AI | PASS/WARN/REJECT 판정 |

## 설치

```powershell
cd C:\Coding\PADO
pip install -r requirements.txt
# .env에 API키 설정
```

## 실행

```powershell
# 스케줄러 (매일 자동)
python main.py

# 전체 테스트 (웹훅 확인)
python main.py --test-all

# 개별 실행
python main.py --briefing     # 아침 브리핑
python main.py --midday        # 장중 시그널
python main.py --cb-pick       # ClosingBell
python main.py --scan          # 장후 파이프라인
```

## 도구

```powershell
# DB 초기화
python tools/reset_db.py

# 과거 소급 (DB에 백데이터 저장)
python tools/backfill.py --days 5 --verify

# 전종목 백테스트 (2016~현재, 연도/섹터/가격대별)
python tools/pullback_backtest.py --output results

# 실전 성과 추적
python tools/pullback_tracker.py --report --days 30

# 개별 종목 시뮬
python tools/case_sim.py --code 003380 263750
```

## DB 테이블

| 테이블 | 용도 |
|--------|------|
| pullback_signals | 눌림목 시그널 + 3차 검증 + D+1~5 수익률 |
| cb_watch_stocks | CB 감시종목 (D+5 만료) |
| news_v2 | Google RSS + 네이버 뉴스 |
| news_analysis | 키워드 분석 + Gemini 추론 |
| jcgs_scan_results | 재차거시 평가 결과 |
| market_daily | 시황 평가 |

## 파일 구조 (45파일, 11,502줄)

```
main.py                          # 스케줄러 + CLI (1,005줄)
config.py                        # 설정 + 상수
closingbell/
  screener.py                    # CB 점수 스크리닝
  entry_watchlist.py             # 유목민 눌림목 감지
jaechageosi/
  formatter.py                   # 디스코드 포맷
  intersection.py                # 4채널 교집합
  material_engine.py             # DART+뉴스+Gemini
  volume_engine.py               # 수급(공매도/거래원)
  market_engine.py               # 시황 평가
shared/
  storage.py                     # DB (SQLite)
  kiwoom_api.py                  # 키움 REST API
  notifier.py                    # 디스코드 웹훅
  stock_map.py                   # 종목 매핑
checkers/
  news_intelligence.py           # 뉴스 수집+분석
  dart_checker.py                # DART 공시
  news_checker.py                # 네이버 뉴스
tools/
  reset_db.py                    # DB 초기화
  backfill.py                    # 과거 소급
  pullback_backtest.py           # 전종목 백테스트
  pullback_tracker.py            # 성과 추적
  case_sim.py                    # 개별 시뮬
```
