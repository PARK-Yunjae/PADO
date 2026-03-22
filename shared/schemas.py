"""공용 데이터 타입 정의."""

from dataclasses import dataclass, field


@dataclass
class StockBasic:
    code: str
    name: str
    market: str           # KOSPI / KOSDAQ
    sector: str
    themes: list[str] = field(default_factory=list)


@dataclass
class OHLCVRow:
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    trading_value: float  # 거래대금


@dataclass
class WaveSignal:
    code: str
    name: str
    wave_type: str        # wave1 / wave2
    detect_date: str
    strength: float       # 0~1
    wave_count: int       # 몇 번째 파동 (약화 판정용)
    obv_bull: bool = False
    gge: bool = False
    reasons: list[str] = field(default_factory=list)
