"""
ClosingBell v4.0 Kiwoom REST API client
==========================================
KIS API를 대체하는 키움 REST API 래퍼.
기존 screener.py와 동일한 인터페이스를 제공하면서
매물대(ka10025), 거래원(ka10038/ka10040), 테마(ka90001) 등
키움 전용 API를 추가로 지원.
"""
import logging
import time
import requests
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("closingbell")


class KiwoomAPI:
    """키움 REST API 클라이언트"""

    def __init__(self, appkey: str, secretkey: str, base_url: str = "https://api.kiwoom.com",
                 api_delay: float = 0.12):
        self.appkey = appkey
        self.secretkey = secretkey
        self.base_url = base_url.rstrip("/")
        self.api_delay = api_delay
        self.token = ""
        self.token_expires = datetime.min

    # ──────────────────────────────────────────────
    # 인증
    # ──────────────────────────────────────────────
    def ensure_token(self):
        """토큰이 없거나 만료 임박 시 재발급"""
        if self.token and datetime.now() < self.token_expires - timedelta(minutes=30):
            return
        self._get_token()

    def _get_token(self):
        """au10001: 접근토큰 발급"""
        resp = requests.post(
            f"{self.base_url}/oauth2/token",
            json={
                "grant_type": "client_credentials",
                "appkey": self.appkey,
                "secretkey": self.secretkey,
            },
            headers={"Content-Type": "application/json;charset=UTF-8"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        self.token = data["token"]
        # expires_dt: "20260308083713" 형식
        exp_str = data.get("expires_dt", "")
        logger.debug("토큰 응답: expires_dt=%s, token_type=%s", exp_str, data.get("token_type"))
        if exp_str and len(exp_str) >= 14:
            try:
                self.token_expires = datetime.strptime(exp_str, "%Y%m%d%H%M%S")
            except ValueError:
                self.token_expires = datetime.now() + timedelta(hours=12)
        else:
            self.token_expires = datetime.now() + timedelta(hours=12)
        logger.info("키움 토큰 발급 완료 (만료: %s)", self.token_expires.strftime("%Y-%m-%d %H:%M"))

    def _headers(self, api_id: str) -> dict:
        """공통 헤더 생성"""
        self.ensure_token()
        return {
            "Content-Type": "application/json;charset=UTF-8",
            "api-id": api_id,
            "authorization": f"Bearer {self.token}",
        }

    def _post(self, url: str, api_id: str, body: dict,
              cont_yn: str = "", next_key: str = "") -> dict:
        """공통 POST 요청"""
        headers = self._headers(api_id)
        if cont_yn:
            headers["cont-yn"] = cont_yn
        if next_key:
            headers["next-key"] = next_key

        time.sleep(self.api_delay)
        resp = requests.post(
            f"{self.base_url}{url}",
            json=body,
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        rc = data.get("return_code", data.get("returnCode", -1))
        if rc != 0:
            msg = data.get("return_msg", data.get("returnMsg", ""))
            raise RuntimeError(f"키움 API 에러 [{api_id}]: {rc} - {msg}")
        return data

    # ──────────────────────────────────────────────
    # 유니버스 확보
    # ──────────────────────────────────────────────
    @staticmethod
    def _clean_code(code: str) -> str:
        """종목코드에서 _AL, _NX 등 거래소 접미사 제거"""
        code = code.strip()
        for suffix in ("_AL", "_NX", "_SOR"):
            if code.endswith(suffix):
                code = code[:-len(suffix)]
        return code.zfill(6)

    def get_volume_rank(self, market: str = "000", min_trading_value: str = "1000",
                        stex_tp: str = "3") -> list[dict]:
        """
        ka10030: 당일거래량상위
        거래대금 100억+ / 관리종목·우선주 제외 / 전체시장
        """
        data = self._post("/api/dostk/rkinfo", "ka10030", {
            "mrkt_tp": market,
            "sort_tp": "1",          # 거래량순
            "mang_stk_incls": "4",   # 관리종목+우선주 제외
            "crd_tp": "0",           # 전체
            "trde_qty_tp": "0",      # 전체
            "pric_tp": "0",          # 전체
            "trde_prica_tp": min_trading_value,  # 100억+
            "mrkt_open_tp": "0",     # 전체
            "stex_tp": stex_tp,      # 통합
        })

        result = []
        for item in data.get("tdy_trde_qty_upper", []):
            result.append(self._parse_rank_item(item))

        # 연속조회로 추가 데이터 수집
        cont = data.get("cont-yn", "")
        nk = data.get("next-key", "")
        page = 0
        while cont == "Y" and page < 3:  # 최대 3페이지
            page += 1
            try:
                data2 = self._post("/api/dostk/rkinfo", "ka10030", {
                    "mrkt_tp": market, "sort_tp": "1", "mang_stk_incls": "4",
                    "crd_tp": "0", "trde_qty_tp": "0", "pric_tp": "0",
                    "trde_prica_tp": min_trading_value, "mrkt_open_tp": "0",
                    "stex_tp": stex_tp,
                }, cont_yn=cont, next_key=nk)
                for item in data2.get("tdy_trde_qty_upper", []):
                    result.append(self._parse_rank_item(item))
                cont = data2.get("cont-yn", "")
                nk = data2.get("next-key", "")
            except Exception:
                break

        logger.info("ka10030 거래량상위: %d종목", len(result))
        return result

    def get_trading_value_rank(self, market: str = "000",
                               stex_tp: str = "3") -> list[dict]:
        """ka10032: 거래대금상위"""
        data = self._post("/api/dostk/rkinfo", "ka10032", {
            "mrkt_tp": market,
            "mang_stk_incls": "0",  # 관리종목 미포함
            "stex_tp": stex_tp,
        })

        result = []
        for item in data.get("trde_prica_upper", []):
            result.append({
                "code": self._clean_code(item.get("stk_cd", "")),
                "name": item.get("stk_nm", "").strip(),
                "price": abs(int(item.get("cur_prc", "0").replace(",", ""))),
                "change_rate": float(item.get("flu_rt", "0")),
                "volume": int(item.get("now_trde_qty", "0").replace(",", "")),
                "trading_value": int(item.get("trde_prica", "0").replace(",", "")),
            })

        # 연속조회
        cont = data.get("cont-yn", "")
        nk = data.get("next-key", "")
        page = 0
        while cont == "Y" and page < 3:
            page += 1
            try:
                data2 = self._post("/api/dostk/rkinfo", "ka10032", {
                    "mrkt_tp": market, "mang_stk_incls": "0", "stex_tp": stex_tp,
                }, cont_yn=cont, next_key=nk)
                for item in data2.get("trde_prica_upper", []):
                    result.append({
                        "code": self._clean_code(item.get("stk_cd", "")),
                        "name": item.get("stk_nm", "").strip(),
                        "price": abs(int(item.get("cur_prc", "0").replace(",", ""))),
                        "change_rate": float(item.get("flu_rt", "0")),
                        "volume": int(item.get("now_trde_qty", "0").replace(",", "")),
                        "trading_value": int(item.get("trde_prica", "0").replace(",", "")),
                    })
                cont = data2.get("cont-yn", "")
                nk = data2.get("next-key", "")
            except Exception:
                break

        logger.info("ka10032 거래대금상위: %d종목", len(result))
        return result

    def _parse_rank_item(self, item: dict) -> dict:
        """거래량상위 응답 파싱 (cur_prc에 부호 포함)"""
        price_str = item.get("cur_prc", "0").replace(",", "")
        return {
            "code": self._clean_code(item.get("stk_cd", "")),
            "name": item.get("stk_nm", "").strip(),
            "price": abs(int(price_str)),
            "change_rate": float(item.get("flu_rt", "0")),
            "volume": int(item.get("trde_qty", "0").replace(",", "")),
            "trading_value": int(item.get("trde_amt", "0").replace(",", "")),
        }

    # ──────────────────────────────────────────────
    # 종목 기본정보 + 현재가
    # ──────────────────────────────────────────────
    def get_stock_info(self, code: str) -> dict:
        """ka10001: 주식기본정보 (시총, PER, EPS, 52주 고저 등)"""
        data = self._post("/api/dostk/stkinfo", "ka10001", {"stk_cd": code})
        return {
            "code": self._clean_code(data.get("stk_cd", code)),
            "name": data.get("stk_nm", ""),
            "market_cap": int(data.get("mac", "0").replace(",", "")),  # 억원
            "per": data.get("per", ""),
            "eps": data.get("eps", ""),
            "roe": data.get("roe", ""),
            "pbr": data.get("pbr", ""),
            "sales": data.get("sale_amt", "0"),
            "operating_profit": data.get("bus_pro", "0"),
            "net_income": data.get("cup_nga", "0"),
            "high_250": data.get("250hgst", ""),
            "low_250": data.get("250lwst", ""),
            "foreign_ratio": data.get("for_exh_rt", "0"),
            "float_shares": data.get("dstr_stk", "0"),
            "float_ratio": data.get("dstr_rt", "0"),
        }

    def get_stock_meta(self, code: str) -> dict:
        """ka10100: 종목정보 조회 (업종명, 시장구분, 투자경고 등)"""
        data = self._post("/api/dostk/stkinfo", "ka10100", {"stk_cd": code})
        return {
            "code": self._clean_code(data.get("code", code)),
            "name": data.get("name", ""),
            "market_name": data.get("marketName", ""),
            "sector": data.get("upName", ""),
            "size": data.get("upSizeName", ""),  # 대형주/중형주/소형주
            "order_warning": data.get("orderWarning", "0"),
            "audit": data.get("auditInfo", ""),
            "status": data.get("state", ""),
        }

    def get_current_price(self, code: str) -> dict:
        """ka10001에서 현재가/시고저 추출"""
        data = self._post("/api/dostk/stkinfo", "ka10001", {"stk_cd": code})
        return {
            "price": abs(int(data.get("cur_prc", "0").replace(",", "") or "0")),
            "change_rate": float(data.get("flu_rt", "0") or "0"),
            "open": abs(int(data.get("open_pric", "0").replace(",", "") or "0")),
            "high": abs(int(data.get("high_pric", "0").replace(",", "") or "0")),
            "low": abs(int(data.get("low_pric", "0").replace(",", "") or "0")),
            "volume": int(data.get("trde_qty", "0").replace(",", "") or "0"),
        }

    # ──────────────────────────────────────────────
    # OHLCV (일봉)
    # ──────────────────────────────────────────────
    def get_daily_ohlcv(self, code: str, base_dt: str = "") -> list[dict]:
        """
        ka10081: 주식일봉차트조회
        base_dt: YYYYMMDD (기본: 오늘)
        최신→과거순으로 반환, 연속조회로 충분한 데이터 확보
        """
        if not base_dt:
            base_dt = datetime.now().strftime("%Y%m%d")

        data = self._post("/api/dostk/chart", "ka10081", {
            "stk_cd": code,
            "base_dt": base_dt,
            "upd_stkpc_tp": "1",  # 수정주가
        })

        rows = []
        for item in data.get("stk_dt_pole_chart_qry", []):
            rows.append(self._parse_ohlcv(item))

        # 연속조회로 더 가져오기 (최소 50일 확보)
        cont = data.get("cont-yn", "")
        nk = data.get("next-key", "")
        while cont == "Y" and len(rows) < 60:
            try:
                data2 = self._post("/api/dostk/chart", "ka10081", {
                    "stk_cd": code, "base_dt": base_dt, "upd_stkpc_tp": "1",
                }, cont_yn=cont, next_key=nk)
                for item in data2.get("stk_dt_pole_chart_qry", []):
                    rows.append(self._parse_ohlcv(item))
                cont = data2.get("cont-yn", "")
                nk = data2.get("next-key", "")
            except Exception:
                break

        return rows

    def _parse_ohlcv(self, item: dict) -> dict:
        """일봉 응답 파싱"""
        return {
            "date": item.get("dt", ""),
            "open": abs(int(item.get("open_pric", "0").replace(",", ""))),
            "high": abs(int(item.get("high_pric", "0").replace(",", ""))),
            "low": abs(int(item.get("low_pric", "0").replace(",", ""))),
            "close": abs(int(item.get("cur_prc", "0").replace(",", ""))),
            "volume": int(item.get("trde_qty", "0").replace(",", "")),
            "trading_value": int(item.get("trde_prica", "0").replace(",", "")),
        }

    # ──────────────────────────────────────────────
    # 업종 지수
    # ──────────────────────────────────────────────
    def get_index_price(self, index_code: str) -> dict:
        """
        ka20001: 업종현재가
        index_code: "001"=코스피, "101"=코스닥
        """
        # 코스피는 mrkt_tp=0, 코스닥은 mrkt_tp=1
        mrkt = "0" if index_code.startswith("0") else "1"
        data = self._post("/api/dostk/sect", "ka20001", {
            "mrkt_tp": mrkt,
            "inds_cd": index_code,
        })
        return {
            "price": abs(float(data.get("cur_prc", "0"))),
            "change_rate": float(data.get("flu_rt", "0")),
            "volume": int(data.get("trde_qty", "0").replace(",", "")),
            "trading_value": int(data.get("trde_prica", "0").replace(",", "")),
        }

    # ══════════════════════════════════════════════
    # v3 전용 — 매물대 / 거래원 / 테마
    # ══════════════════════════════════════════════

    def get_volume_profile(self, code: str, cycle: int = 100,
                           bands: int = 10) -> list[dict]:
        """
        ka10025: 매물대집중요청
        종목의 가격대별 매물량 분포 반환
        """
        # ka10025는 종목코드를 직접 받지 않고 시장 전체 스캔이므로
        # 시장구분+필터로 조회 후 해당 종목만 필터링
        # → 실제로는 개별 종목용 아닌 시장 랭킹 API
        # 대신 OHLCV 데이터로 자체 계산하거나, ka10043(거래원매물대)을 활용

        # ka10025는 시장 전체 매물대 집중 종목을 반환하므로
        # 개별 종목 매물대는 OHLCV 기반 자체 계산이 더 정확
        # 여기서는 자체 계산 로직 제공
        return []  # screener.py에서 OHLCV 기반으로 직접 계산

    def get_volume_profile_market(self, market: str = "000", cycle: int = 100,
                                   bands: int = 10, conc_rate: int = 70) -> list[dict]:
        """
        ka10025: 매물대집중요청 (시장 전체 스캔)
        매물대가 집중된 종목 리스트 반환
        """
        data = self._post("/api/dostk/stkinfo", "ka10025", {
            "mrkt_tp": market,
            "prps_cnctr_rt": str(conc_rate),
            "cur_prc_entry": "0",
            "prpscnt": str(bands),
            "cycle_tp": str(cycle),
            "stex_tp": "3",
        })
        result = []
        for item in data.get("prps_cnctr", []):
            result.append({
                "code": self._clean_code(item.get("stk_cd", "")),
                "name": item.get("stk_nm", ""),
                "price": abs(int(item.get("cur_prc", "0").replace(",", ""))),
                "change_rate": float(item.get("flu_rt", "0")),
                "band_start": int(item.get("pric_strt", "0").replace(",", "")),
                "band_end": int(item.get("pric_end", "0").replace(",", "")),
                "band_volume": int(item.get("prps_qty", "0").replace(",", "")),
                "band_ratio": float(item.get("prps_rt", "0").replace("+", "")),
            })
        return result

    def get_broker_ranking(self, code: str, period: str = "1",
                           sort: str = "2") -> dict:
        """
        ka10038: 종목별증권사순위
        period: "1"=전일, "4"=5일, "9"=10일
        sort: "1"=순매도순, "2"=순매수순
        """
        today = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")

        data = self._post("/api/dostk/rkinfo", "ka10038", {
            "stk_cd": code,
            "strt_dt": start,
            "end_dt": today,
            "qry_tp": sort,
            "dt": period,
        })

        brokers = []
        for item in data.get("stk_sec_rank", []):
            brokers.append({
                "rank": int(item.get("rank", "0")),
                "name": item.get("mmcm_nm", "").strip(),
                "buy": int(item.get("buy_qty", "0").replace("+", "").replace("-", "").replace(",", "")),
                "sell": int(item.get("sell_qty", "0").replace("+", "").replace("-", "").replace(",", "")),
                "net": int(item.get("acc_netprps_qty", "0").replace("+", "").replace(",", "").replace("-", "-")),
            })

        return {
            "total_buy": data.get("rank_1", "0"),
            "total_sell": data.get("rank_2", "0"),
            "net_total": data.get("rank_3", "0"),
            "brokers": brokers,
        }

    def get_broker_detail(self, code: str) -> dict:
        """
        ka10040: 당일주요거래원
        매수/매도 TOP5 증권사 + 외국계 순매수 합계
        """
        data = self._post("/api/dostk/rkinfo", "ka10040", {"stk_cd": code})

        buy_brokers = []
        sell_brokers = []
        for i in range(1, 6):
            buy_name = data.get(f"buy_trde_ori_{i}", "").strip()
            sell_name = data.get(f"sel_trde_ori_{i}", "").strip()
            if buy_name:
                buy_brokers.append({
                    "name": buy_name,
                    "code": data.get(f"buy_trde_ori_cd_{i}", ""),
                    "qty": int(data.get(f"buy_trde_ori_qty_{i}", "0")
                               .replace("+", "").replace(",", "")),
                })
            if sell_name:
                sell_brokers.append({
                    "name": sell_name,
                    "code": data.get(f"sel_trde_ori_cd_{i}", ""),
                    "qty": int(data.get(f"sel_trde_ori_qty_{i}", "0")
                               .replace("-", "").replace(",", "")),
                })

        frgn_buy_str = data.get("frgn_buy_prsm_sum", "0").replace("+", "").replace(",", "")
        frgn_sell_str = data.get("frgn_sel_prsm_sum", "0").replace("-", "").replace(",", "")

        return {
            "buy_top5": buy_brokers,
            "sell_top5": sell_brokers,
            "foreign_buy_total": int(frgn_buy_str) if frgn_buy_str else 0,
            "foreign_sell_total": int(frgn_sell_str) if frgn_sell_str else 0,
            "foreign_net": int(data.get("frgn_buy_prsm_sum", "0")
                               .replace("+", "").replace(",", "") or "0")
                         - int(data.get("frgn_sel_prsm_sum", "0")
                               .replace("-", "").replace(",", "") or "0"),
        }

    def get_broker_volume_profile(self, code: str, broker_code: str = "",
                                   period: str = "5") -> list[dict]:
        """
        ka10043: 거래원매물대분석
        특정 증권사의 가격대별 매수/매도 분석
        """
        today = datetime.now().strftime("%Y%m%d")
        data = self._post("/api/dostk/stkinfo", "ka10043", {
            "stk_cd": code,
            "strt_dt": today,
            "end_dt": today,
            "qry_dt_tp": "0",        # 기간으로 조회
            "pot_tp": "0",            # 당일
            "dt": period,
            "sort_base": "1",         # 종가순
            "mmcm_cd": broker_code,   # 비어있으면 전체
            "stex_tp": "3",
        })

        result = []
        for item in data.get("trde_ori_prps_anly", []):
            result.append({
                "date": item.get("dt", ""),
                "close": abs(int(item.get("close_pric", "0").replace(",", ""))),
                "sell_qty": int(item.get("sel_qty", "0").replace(",", "")),
                "buy_qty": int(item.get("buy_qty", "0").replace(",", "")),
                "net_qty": int(item.get("netprps_qty", "0").replace(",", "")),
                "trade_weight": float(item.get("trde_wght", "0").replace("+", "")),
            })
        return result

    # ──────────────────────────────────────────────
    # 테마 / 주도섹터
    # ──────────────────────────────────────────────
    def get_theme_groups(self, sort: str = "3", period: str = "1") -> list[dict]:
        """
        ka90001: 테마그룹별
        sort: "1"=기간수익률↑, "3"=등락률↑
        """
        data = self._post("/api/dostk/thme", "ka90001", {
            "qry_tp": "0",            # 전체검색
            "stk_cd": "",
            "date_tp": period,
            "thema_nm": "",
            "flu_pl_amt_tp": sort,
            "stex_tp": "3",
        })

        result = []
        for item in data.get("thema_grp", []):
            result.append({
                "code": item.get("thema_grp_cd", ""),
                "name": item.get("thema_nm", ""),
                "stock_count": int(item.get("stk_num", "0")),
                "change_rate": float(item.get("flu_rt", "0")),
                "rising_count": int(item.get("rising_stk_num", "0")),
                "falling_count": int(item.get("fall_stk_num", "0")),
                "period_return": float(item.get("dt_prft_rt", "0")),
                "main_stock": item.get("main_stk", ""),
            })
        return result

    def get_stock_themes(self, code: str) -> list[dict]:
        """
        ka90001(종목검색): 특정 종목이 속한 테마 목록 + 오늘 등락률.
        qry_tp="2"로 종목코드 기준 검색.
        """
        data = self._post("/api/dostk/thme", "ka90001", {
            "qry_tp": "2",            # 종목검색
            "stk_cd": code,
            "date_tp": "1",
            "thema_nm": "",
            "flu_pl_amt_tp": "3",     # 등락률순
            "stex_tp": "3",
        })

        result = []
        for item in data.get("thema_grp", []):
            result.append({
                "code": item.get("thema_grp_cd", ""),
                "name": item.get("thema_nm", ""),
                "change_rate": float(item.get("flu_rt", "0")),
                "rising_count": int(item.get("rising_stk_num", "0")),
                "falling_count": int(item.get("fall_stk_num", "0")),
            })
        return result

    def get_theme_stocks(self, theme_code: str) -> list[dict]:
        """ka90002: 테마구성종목"""
        data = self._post("/api/dostk/thme", "ka90002", {
            "date_tp": "1",
            "thema_grp_cd": theme_code,
            "stex_tp": "3",
        })
        result = []
        for item in data.get("thema_comp_stk", []):
            result.append({
                "code": self._clean_code(item.get("stk_cd", "")),
                "name": item.get("stk_nm", ""),
                "price": abs(int(item.get("cur_prc", "0").replace(",", ""))),
                "change_rate": float(item.get("flu_rt", "0")),
                "volume": int(item.get("acc_trde_qty", "0").replace(",", "")),
            })
        return result

    # ──────────────────────────────────────────────
    # 수급 분석 API (재차거시 — 거·시 강화)
    # ──────────────────────────────────────────────
    def get_short_selling(self, code: str, days: int = 5) -> list[dict]:
        """
        ka10014: 공매도추이요청
        최근 N일 공매도량·매매비중 반환.
        """
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=days + 10)).strftime("%Y%m%d")

        data = self._post("/api/dostk/shsa", "ka10014", {
            "stk_cd": code,
            "tm_tp": "1",
            "strt_dt": start,
            "end_dt": end,
        })

        result = []
        for item in data.get("shrts_trnsn", [])[:days]:
            close_str = str(item.get("close_pric", item.get("cur_prc", "0")) or "0")
            result.append({
                "date": item.get("dt", ""),
                "close": abs(int(close_str.replace(",", ""))),
                "short_qty": int(item.get("shrts_qty", "0").replace(",", "")),
                "trade_qty": int(item.get("trde_qty", "0").replace(",", "")),
                "short_ratio": float(item.get("trde_wght", "0")
                                     .replace("+", "").replace(",", "") or "0"),
                "short_value": int(item.get("shrts_trde_prica", "0").replace(",", "")),
            })
        return result

    def get_stock_lending(self, code: str, days: int = 5) -> list[dict]:
        """
        ka20068: 대차거래추이요청(종목별)
        대차잔고 증감 추이 반환.
        """
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=days + 10)).strftime("%Y%m%d")

        data = self._post("/api/dostk/slb", "ka20068", {
            "strt_dt": start,
            "end_dt": end,
            "all_tp": "0",
            "stk_cd": code,
        })

        result = []
        for item in data.get("dbrt_trde_trnsn", [])[:days]:
            result.append({
                "date": item.get("dt", ""),
                "lend_qty": int(item.get("dbrt_trde_cntrcnt", "0").replace(",", "")),
                "return_qty": int(item.get("dbrt_trde_rpy", "0").replace(",", "")),
                "change": int(item.get("dbrt_trde_irds", "0").replace(",", "")),
                "balance": int(item.get("rmnd", "0").replace(",", "")),
            })
        return result

    def get_credit_trend(self, code: str) -> list[dict]:
        """
        ka10013: 신용매매동향요청 (융자)
        신용잔고·잔고율 추이 반환.
        """
        dt = datetime.now().strftime("%Y%m%d")

        data = self._post("/api/dostk/stkinfo", "ka10013", {
            "stk_cd": code,
            "dt": dt,
            "qry_tp": "1",  # 융자
        })

        result = []
        for item in data.get("crd_trde_trend", [])[:10]:
            result.append({
                "date": item.get("dt", ""),
                "new": int(item.get("new", "0").replace(",", "") or "0"),
                "repay": int(item.get("rpya", "0").replace(",", "") or "0"),
                "balance": int(item.get("remn", "0").replace(",", "") or "0"),
                "balance_ratio": float(item.get("remn_rt", "0").replace(",", "") or "0"),
            })
        return result

    def get_investor_trend(self, code: str, days: int = 5) -> list[dict]:
        """
        ka10059: 종목별투자자기관별요청
        외인·기관·개인 순매수 일별 추이 반환.
        """
        dt = datetime.now().strftime("%Y%m%d")

        data = self._post("/api/dostk/stkinfo", "ka10059", {
            "dt": dt,
            "stk_cd": code,
            "amt_qty_tp": "2",   # 수량
            "trde_tp": "0",      # 순매수
            "unit_tp": "1",      # 단주
        })

        result = []
        for item in data.get("stk_invsr_orgn", [])[:days]:
            result.append({
                "date": item.get("dt", ""),
                "individual": int(item.get("ind_invsr", "0").replace(",", "") or "0"),
                "foreign": int(item.get("frgnr_invsr", "0").replace(",", "") or "0"),
                "institution": int(item.get("orgn", "0").replace(",", "") or "0"),
            })
        return result

    def get_execution_strength(self, code: str) -> list[dict]:
        """
        ka10047: 체결강도추이일별요청
        매수세 vs 매도세 강도 반환.
        """
        data = self._post("/api/dostk/mrkcond", "ka10047", {
            "stk_cd": code,
        })

        result = []
        for item in data.get("cntr_str_daly", [])[:5]:
            result.append({
                "date": item.get("dt", ""),
                "strength": float(item.get("cntr_str", "0") or "0"),
                "strength_5d": float(item.get("cntr_str_5min", "0") or "0"),
                "strength_20d": float(item.get("cntr_str_20min", "0") or "0"),
            })
        return result

    def get_foreign_exhaust_rank(self, market: str = "000",
                                 period: str = "0") -> list[dict]:
        """
        ka10036: 외인한도소진율증가상위
        market: "000"=전체, "001"=코스피, "101"=코스닥
        period: "0"=당일, "1"=전일, "5"=5일, "20"=20일
        """
        data = self._post("/api/dostk/rkinfo", "ka10036", {
            "mrkt_tp": market,
            "dt": period,
            "stex_tp": "3",
        })

        result = []
        for item in data.get("for_limit_exh_rt_incrs_upper", []):
            result.append({
                "rank": int(item.get("rank", "0")),
                "code": self._clean_code(item.get("stk_cd", "")),
                "name": item.get("stk_nm", "").strip(),
                "price": abs(int(item.get("cur_prc", "0").replace(",", ""))),
                "held_shares": int(item.get("poss_stkcnt", "0").replace(",", "")),
                "available_shares": int(item.get("gain_pos_stkcnt", "0").replace(",", "")),
                "exhaust_rate": float(
                    item.get("base_limit_exh_rt", "0").replace(",", "").replace("+", "") or "0"
                ),
            })
        return result

    def get_foreign_daily(self, code: str) -> list[dict]:
        """
        ka10008: 주식외국인종목별매매동향
        종목별 외국인 보유/한도소진율 일별 추이.
        """
        data = self._post("/api/dostk/frgnistt", "ka10008", {
            "stk_cd": code,
        })

        result = []
        for item in data.get("stk_frgnr", []):
            result.append({
                "date": item.get("dt", ""),
                "close": abs(int(item.get("close_pric", "0").replace(",", ""))),
                "change_qty": int(item.get("chg_qty", "0").replace(",", "")),
                "held_shares": int(item.get("poss_stkcnt", "0").replace(",", "")),
                "weight_pct": float(item.get("wght", "0").replace(",", "") or "0"),
                "exhaust_rate": float(
                    item.get("limit_exh_rt", "0").replace(",", "").replace("+", "") or "0"
                ),
            })
        return result

    def get_minute_chart(self, code: str, interval: str = "15") -> list[dict]:
        """
        ka10080: 주식분봉차트조회요청
        interval: "1","3","5","10","15","30","45","60"
        """
        data = self._post("/api/dostk/chart", "ka10080", {
            "stk_cd": code,
            "tic_scope": interval,
            "upd_stkpc_tp": "1",
        })

        result = []
        for item in data.get("stk_min_pole_chart_qry", []):
            result.append({
                "time": item.get("cntr_tm", ""),
                "open": abs(int(item.get("open_pric", "0").replace(",", ""))),
                "high": abs(int(item.get("high_pric", "0").replace(",", ""))),
                "low": abs(int(item.get("low_pric", "0").replace(",", ""))),
                "close": abs(int(item.get("cur_prc", "0").replace(",", ""))),
                "volume": int(item.get("trde_qty", "0").replace(",", "")),
            })
        return result
