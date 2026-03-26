"""재차거시 4채널 결과 데이터 타입."""

from dataclasses import dataclass, field
from typing import Literal

ChartState = Literal["bottom", "pullback", "breakout", "extended"]
FlowState = Literal["accumulation", "ignite", "digest", "reignite", "chasing"]
Freshness = Literal["first_seen", "recent", "stale"]
CatalystType = Literal["contract", "policy", "earnings", "theme", "relief", "unknown"]
MarketMode = Literal["theme_strong", "index_rally", "mixed", "risk_off"]
Grade = Literal["A", "B", "C", "REJECT"]


@dataclass
class ChartResult:
    code: str
    score: int                                  # 0~100
    signal_family: list[str]                    # ["wave1","gc","rsi_reclaim"]
    chart_state: ChartState
    ma_alignment: str                           # 정배열/혼합/역배열
    rsi: float
    nearest_support: float
    nearest_resistance: float
    gc_days_ago: int | None                     # GC 경과일 (None=미발생)
    entry_price: float | None                   # max(ma8,ma33)×1.01
    stop_loss: float | None                     # ma33×0.97
    target_price: float | None                  # nearest_resistance×0.98
    reasons: list[str] = field(default_factory=list)


@dataclass
class VolumeResult:
    code: str
    score: int                                  # 0~100
    flow_state: FlowState
    obv_bull_div: bool
    gge_strict: bool                            # 거감음봉 + 5일선이격≤5%
    dryup_days: int
    explosion_ratio: float
    inst_foreign_5d: bool
    short_ratio: float
    trap_flag: bool                             # 양봉폭발+윗꼬리+둔화
    reasons: list[str] = field(default_factory=list)


@dataclass
class MaterialResult:
    code: str
    score: int                                  # 0~100
    catalyst_type: CatalystType
    freshness: Freshness
    theme_link: str                             # 관련 테마 (빈 문자열=없음)
    dart_grade: int                             # 1~6 (극위험~강호재)
    headline_summary: str
    decay_risk: str                             # high/mid/low
    reasons: list[str] = field(default_factory=list)


@dataclass
class MarketResult:
    date: str
    score: int                                  # 0~100
    mode: MarketMode
    leading_themes: list[str]
    dangerous: bool                             # True → 전원 REJECT
    nasdaq_change: float
    kospi_ma20_gap: float
    seasonal_note: str
    reasons: list[str] = field(default_factory=list)


@dataclass
class IntersectionResult:
    code: str
    name: str
    grade: Grade
    confidence: int                             # 0~100
    action: str                                 # pullback_entry/watch/record_only/reject
    reject_reason: str | None
    chart: ChartResult
    volume: VolumeResult
    material: MaterialResult
    market: MarketResult
    theme_match_bonus: int = 0
    synergy_bonus: int = 0
    # v4.1: 시그널 분류
    signal_type: str = "score_only"             # wave_plus_score / rsi_reversal / score_only
    recommended_hold_days: str = "D+3~5"        # D+5 / D+15~20 / D+3~5
    strategy_bucket: str = "pullback"           # wave / hold / pullback
