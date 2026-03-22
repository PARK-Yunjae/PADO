# PADO (파도)

> 바닥에서 밀려오는 파동을 잡는다.

한국 주식 자동 스크리닝 시스템. **재차거시**(재료·차트·거래량·시황) 4채널 교집합으로 매수 후보를 탐색하고, 파동 감지·ClosingBell 눌림목 추천·디스코드 알림까지 하나로 통합.

## 핵심 아이디어

유목민 투자법의 핵심을 자동화:
1. **역사적 저점** 부근 종목을 차트로 발굴
2. **거래량 급감 → 폭발** 패턴으로 자금 유입 확인
3. **시황**(오늘 시장이 어떤 섹터에 돈이 몰리나)과 교차 검증
4. **재료**(공시·뉴스·AI 분석)로 이유 설명 + 악재 제거
5. 4채널 모두 통과한 종목만 **A/B/C 등급** 부여

즉시 진입이 아니라 **감시 등록 → D+2~3 눌림목 대기** 방식.

## 프로젝트 구조

```
PADO/
├── jaechageosi/       재차거시 4채널 엔진 (핵심)
│   ├── chart_engine      전종목 OHLCV 스캔 → 신호 탐지 + 100점
│   ├── volume_engine     거래량 패턴 + 수급 API → 100점
│   ├── market_engine     3소스 병합 시황 → 100점
│   ├── material_engine   DART+뉴스+Gemini → 100점
│   └── intersection      4채널 AND → A/B/C 등급
│
├── closingbell/       ClosingBell (형님용 순수 차트 TOP3)
├── wave/              파동 감시 (1차/2차 파동 감지)
│
├── shared/            공유 모듈
│   ├── theme_taxonomy    하이브리드 테마 분류 (16canon + 8mega)
│   ├── ohlcv_cache       OHLCV 싱글톤 캐시 + 거래대금 사이드카
│   ├── kiwoom_api        키움증권 REST API
│   ├── storage           SQLite (pado.db)
│   └── notifier          디스코드 웹훅
│
├── checkers/          외부 데이터
│   ├── ai_analyzer       Gemini 섹터별 프롬프트 6+1종
│   ├── news_collector    뉴스 매일 수집
│   ├── dart_checker      DART 공시 6단계
│   └── news_checker      종목별 뉴스 검색
│
├── monitor/           시장 컨텍스트, 성과 추적, 캘린더
├── updater/           OHLCV 갱신, 주간 업데이트
├── tools/             백테스트, 검증 스크립트
├── scripts/           윈도우 작업 스케줄러 BAT
├── main.py            통합 스케줄러 + CLI
└── config.py          .env 기반 설정
```

## 일일 스케줄

| 시간 | 작업 | 디스코드 |
|------|------|----------|
| 08:25 | 아침 브리핑 | 🔍 전일 A/B등급 + 🌊 파동 알림 |
| 14:00 | 장중 체크 | 📍 눌림목 진입 포착 |
| 15:00 | ClosingBell | 🎯 형님용 TOP3 |
| 15:35 | 파이프라인 | DB 저장 (웹훅은 다음날 브리핑) |

주말/공휴일: 뉴스 수집 + 글로벌 지수만 자동 실행.

## 파이프라인 상세 (15:35)

```
① OHLCV 갱신 (FinanceDataReader)
①-b OHLCV 캐시 로드 (~8초, 거래대금 사이드카 동시)
② 글로벌 지수 (나스닥, 코스피, VIX 등)
③ 성과 추적 (D+1~5 수익률)
④ 차트 스캔 (2,881종목 → ~60건 후보)
⑤ 거래량 필터 (60건 → 15~25건)
⑥ 시황 3소스 병합
   ├ 소스1: 키움 테마 API
   ├ 소스2: 거래대금 변화율 TOP (OHLCV, API 0콜)
   └ 소스3: 뉴스 키워드 빈도
⑦ 재료 (DART + 뉴스 + Gemini 섹터별 프롬프트)
⑧ 교집합 → A/B/C 등급 + 테마 매칭 보너스
⑨ DB 저장 + 감시 등록
⑩ 파동 스캔 (1차/2차)
⑪ 주간 업데이트 (월요일)
⑫ 뉴스 수집 (매일 200~350건 축적)
```

## 테마 분류 — 3소스가 같은 말을 하게 만들기

키움 API는 "HBM수혜주", stock_mapping은 "반도체 제조업", 뉴스는 "반도체" — 전부 같은 뜻인데 다른 이름.

```
하이브리드 분류 체계:
  Level 1: 8개 mega-theme (소스 간 일치 판정)
  Level 2: 16개 canon (세분류)
  Level 3: 키워드 (키움 300+, KSIC 163개)

키움 "HBM수혜주"    → canon "반도체" → mega "반도체_IT"
KSIC "반도체 제조업"  → canon "반도체" → mega "반도체_IT"
뉴스 "반도체"        → canon "반도체" → mega "반도체_IT"
→ 3소스 일치 = 시황 35점 만점
```

## 백테스트 결과 (6개월, 377건)

재료 없이 차트+거래량+시황만으로 측정한 결과:

**최강 조합: `bottom + ignite` (바닥에서 거래량 폭발)**
- D+2: **+3.59%**, 승률 59%

**시황 방향 일치하는 B등급:**
- D+2: **+2.68%**, 승률 56%

| 조건 | D+2 | 승률 | 해석 |
|------|------|------|------|
| bottom + ignite | +3.59% | 59% | 유목민 핵심 패턴 |
| pullback + accumulation | +1.46% | 53% | 눌림목 자금 축적 |
| B등급 + 테마매칭 | +2.68% | 56% | 시황과 방향 일치 |
| breakout | +0.04% | 39% | 돌파 추격 = 에지 없음 |
| digest | -1.80% | 30% | 거래량 급감 = 위험 |

## 설치

```bash
git clone https://github.com/사용자/PADO.git
cd PADO
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
copy .env.example .env          # API 키 채우기
```

## 사용법

```bash
# 전체 테스트 (웹훅 전부 발송)
python main.py --test-all

# 일상 사용
python main.py --once         # 거래일 자동 판단 (주말이면 뉴스만)
python main.py --scan         # 파이프라인만
python main.py --briefing     # 아침 브리핑만
python main.py --cb-pick      # ClosingBell TOP3만
python main.py --weekend      # 비거래일 모드

# 백테스트
python tools/jcgs_backtest.py                     # 최근 3개월
python tools/jcgs_backtest.py --start 2025-09-01  # 6개월
python tools/jcgs_backtest.py --step 1            # 매일 스캔

# 검증
python tools/v2_verify.py         # 설정 검증
python tools/weekend_test.py      # 통합 테스트
```

## 윈도우 작업 스케줄러

`scripts/` 폴더의 BAT 파일을 작업 스케줄러에 등록:

| BAT | 트리거 | 용도 |
|-----|--------|------|
| `pado_daily.bat` | 매일 08:20 | 거래일/주말 자동 판단 |
| `pado_briefing.bat` | 평일 08:25 | 아침 브리핑 웹훅 |
| `pado_scan.bat` | 평일 15:35 | 전체 파이프라인 |

PC 자동 전원: **08:15** 기동 권장.

## 필요 API

| API | 용도 | Tier |
|-----|------|------|
| 키움증권 REST | 테마, 수급, 현재가 | 1 (필수) |
| DART | 공시 조회 | 1 (필수) |
| 네이버 검색 | 뉴스 수집 | 2 (대체 가능) |
| Gemini | 재료 AI 분석 | 2 (대체 가능) |

## 코드 규모

38파일, 7,511줄 (v2 기준)

## 면책

개인 학습·연구 목적 프로젝트입니다. 투자 판단의 근거로 사용하지 마세요. 모든 투자 책임은 본인에게 있습니다.
