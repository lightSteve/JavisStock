"""
데이터 수집 모듈 (Data Fetcher) — Naver Finance API 기반
KRX 데이터 포탈이 2026년부터 로그인을 요구하므로 Naver Finance API로 전환.

주요 데이터 소스:
- Naver 모바일 API  : 전종목 시세, 개별 종목 OHLCV, 투자자 동향
- Naver Finance HTML : 업종(섹터) 매핑
- 로컬 CSV 스냅샷    : 수급 데이터 일일 누적 (5일 제한 극복)
"""

import datetime
import json
import os
import re
import time
import requests
import pandas as pd
import numpy as np
from typing import List, Dict

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------
NAVER_API = "https://m.stock.naver.com/api"
NAVER_FINANCE = "https://finance.naver.com"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}
_PAGE_SIZE = 100

_session = requests.Session()
_session.headers.update(_HEADERS)

# 인메모리 캐시 (앱 실행 주기 동안 유지)
_cache: Dict[str, object] = {}


# ---------------------------------------------------------------------------
# 파싱 유틸리티
# ---------------------------------------------------------------------------

def _to_int(val) -> int:
    """쉼표가 포함된 양수 문자열 → 정수."""
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    try:
        return int(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0


def _to_signed_int(val) -> int:
    """부호(+/-)가 포함된 쉼표 문자열 → 정수 (부호 보존)."""
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    try:
        return int(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0


def _to_float(val) -> float:
    """쉼표가 포함된 문자열 → 실수."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Naver 모바일 API — 내부 호출 함수
# ---------------------------------------------------------------------------

def _fetch_all_stocks_raw(market: str) -> List[Dict]:
    """시가총액순 전종목 데이터 수집 (인메모리 캐시)."""
    cache_key = f"stocks_{market}"
    if cache_key in _cache:
        return _cache[cache_key]

    all_stocks: List[Dict] = []
    page = 1
    while True:
        url = f"{NAVER_API}/stocks/marketValue/{market}"
        params = {"page": page, "pageSize": _PAGE_SIZE}
        try:
            resp = _session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[WARN] Naver API 호출 실패 ({market} page={page}): {e}")
            break

        stocks = data.get("stocks", [])
        if not stocks:
            break
        all_stocks.extend(stocks)

        total = data.get("totalCount", 0)
        if len(all_stocks) >= total or len(stocks) < _PAGE_SIZE:
            break
        page += 1
        time.sleep(0.05)

    _cache[cache_key] = all_stocks
    return all_stocks


def _fetch_stock_price_history(ticker: str, count: int = 120) -> List[Dict]:
    """개별 종목 일별 시세 (최대 count 거래일)."""
    all_data: List[Dict] = []
    page = 1
    page_size = min(count, 60)          # Naver API 최대 pageSize = 60
    while len(all_data) < count:
        url = f"{NAVER_API}/stock/{ticker}/price"
        params = {"pageSize": page_size, "page": page}
        try:
            resp = _session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            items = resp.json()
        except Exception:
            break
        if not isinstance(items, list) or not items:
            break
        all_data.extend(items)
        if len(items) < page_size:
            break
        page += 1
        time.sleep(0.1)
    return all_data[:count]


def _fetch_stock_integration(ticker: str) -> Dict:
    """종목 통합 정보 (OHLCV, 수급, 업종 등)."""
    cache_key = f"integration_{ticker}"
    if cache_key in _cache:
        return _cache[cache_key]

    url = f"{NAVER_API}/stock/{ticker}/integration"
    try:
        resp = _session.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        _cache[cache_key] = data
        return data
    except Exception:
        return {}


def clear_integration_cache(ticker: str = None):
    """integration 캐시 클리어. ticker=None이면 전체."""
    if ticker:
        _cache.pop(f"integration_{ticker}", None)
    else:
        keys = [k for k in _cache if k.startswith("integration_")]
        for k in keys:
            del _cache[k]


# ---------------------------------------------------------------------------
# 공개 API : 최근 거래일
# ---------------------------------------------------------------------------

def get_latest_trading_date() -> str:
    """가장 최근 거래일을 YYYYMMDD 형태로 반환."""
    today = datetime.date.today()
    now = datetime.datetime.now()
    dt = today
    # 장 마감(16시) 전이면 전일 기준
    if now.hour < 16:
        dt -= datetime.timedelta(days=1)
    # 주말 건너뛰기
    while dt.weekday() >= 5:
        dt -= datetime.timedelta(days=1)
    return dt.strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# 공개 API : 전종목 티커 & 종목명
# ---------------------------------------------------------------------------

def get_all_tickers(date: str, market: str = "ALL") -> pd.DataFrame:
    """
    KOSPI/KOSDAQ 전종목 티커·종목명.
    반환 컬럼: 티커, 종목명, 시장
    """
    markets = ["KOSPI", "KOSDAQ"] if market == "ALL" else [market]
    rows = []
    for mkt in markets:
        for s in _fetch_all_stocks_raw(mkt):
            rows.append({
                "티커": s.get("itemCode", ""),
                "종목명": s.get("stockName", ""),
                "시장": mkt,
                "종목유형": s.get("stockEndType", "stock"),
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 공개 API : 시세 데이터
# ---------------------------------------------------------------------------

def get_market_ohlcv(date: str, market: str = "ALL") -> pd.DataFrame:
    """
    전종목 시세 조회 (Naver API 최신 데이터).
    컬럼: 시가, 고가, 저가, 종가, 거래량, 거래대금(원), 등락률, 시장, 시가총액

    Note: Naver 대량 API는 장중 시가/고가/저가를 별도 제공하지 않으므로
          종가/전일종가 기반 근사값을 사용합니다.
          정확한 OHLCV가 필요하면 get_stock_ohlcv_history() 를 사용하세요.
    """
    markets = ["KOSPI", "KOSDAQ"] if market == "ALL" else [market]
    rows = []
    for mkt in markets:
        for s in _fetch_all_stocks_raw(mkt):
            close = _to_int(s.get("closePrice", 0))
            if close == 0:
                continue  # 거래 정지 종목 제외
            change = _to_signed_int(s.get("compareToPreviousClosePrice", 0))
            prev_close = close - change
            # 등락률: API가 0을 반환하면(장 외 시간) 전일대비 가격으로 직접 계산
            frate = _to_float(s.get("fluctuationsRatio", 0))
            if frate == 0 and prev_close > 0 and change != 0:
                frate = round((change / prev_close) * 100, 2)
            # Naver 거래대금은 백만원 단위 → 원으로 변환
            tv_raw = _to_int(s.get("accumulatedTradingValue", 0))
            rows.append({
                "티커": s.get("itemCode", ""),
                "시가": prev_close,      # 전일종가 (근사값)
                "고가": close,            # 근사값
                "저가": close,            # 근사값
                "종가": close,
                "거래량": _to_int(s.get("accumulatedTradingVolume", 0)),
                "거래대금": tv_raw * 1_000_000,
                "등락률": frate,
                "시장": mkt,
                "시가총액": _to_int(s.get("marketValue", 0)) * 100_000_000,
                "종목유형": s.get("stockEndType", "stock"),
            })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).set_index("티커")
    return df


def get_stock_ohlcv_history(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    개별 종목 일봉 히스토리 (정확한 OHLCV).
    반환: index=날짜(DatetimeIndex), columns=[시가, 고가, 저가, 종가, 거래량, 등락률]
    """
    start_dt = datetime.datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.datetime.strptime(end, "%Y%m%d")
    days_needed = (end_dt - start_dt).days
    trading_days = int(days_needed * 0.75) + 15  # 영업일 수 근사 + 여유

    items = _fetch_stock_price_history(ticker, count=trading_days)
    if not items:
        return pd.DataFrame()

    rows = []
    for d in items:
        date_str = d.get("localTradedAt", "")
        if not date_str:
            continue
        rows.append({
            "날짜": date_str,
            "시가": _to_int(d.get("openPrice", 0)),
            "고가": _to_int(d.get("highPrice", 0)),
            "저가": _to_int(d.get("lowPrice", 0)),
            "종가": _to_int(d.get("closePrice", 0)),
            "거래량": _to_int(d.get("accumulatedTradingVolume", 0)),
            "등락률": _to_float(d.get("fluctuationsRatio", 0)),
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["날짜"] = pd.to_datetime(df["날짜"])
    df = df.set_index("날짜").sort_index()
    mask = (df.index >= pd.Timestamp(start_dt)) & (df.index <= pd.Timestamp(end_dt))
    return df[mask]


def get_stock_name(ticker: str) -> str:
    """종목코드 → 종목명 (캐시 우선, 없으면 개별 조회)."""
    for key, val in _cache.items():
        if key.startswith("stocks_") and isinstance(val, list):
            for s in val:
                if s.get("itemCode") == ticker:
                    return s.get("stockName", ticker)
    data = _fetch_stock_integration(ticker)
    return data.get("stockName", ticker)


def get_realtime_price(ticker: str) -> dict:
    """종목의 실시간 현재가/등락률을 캐시 없이 조회 (basic 엔드포인트).
    반환: {"price": int, "change_rate": float, "name": str}
    """
    url = f"{NAVER_API}/stock/{ticker}/basic"
    try:
        resp = _session.get(url, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        close = _to_int(data.get("closePrice", 0))
        rate = float(data.get("fluctuationsRatio", 0) or 0)
        name = data.get("stockName", ticker)
        return {"price": close, "change_rate": rate, "name": name}
    except Exception:
        return {"price": 0, "change_rate": 0.0, "name": ticker}


# ---------------------------------------------------------------------------
# 공개 API : 투자자별 순매수 (수급)
# ---------------------------------------------------------------------------

def get_trading_value_by_investor(date: str, market: str = "ALL") -> pd.DataFrame:
    """전종목 투자자별 순매수 — Naver 벌크 미지원으로 빈 DataFrame 반환."""
    return pd.DataFrame()


def get_accumulated_investor_trading(
    end_date: str, days: int = 5, market: str = "ALL"
) -> pd.DataFrame:
    """
    거래대금 상위 200종목의 최근 N거래일 누적 기관/외국인/개인 순매수(원)
    Naver integration 엔드포인트(최대 5일)를 활용합니다.
    반환: index=티커, columns=[기관합계, 외국인합계, 개인]
    """
    ohlcv = get_market_ohlcv(end_date, market)
    if ohlcv.empty:
        return pd.DataFrame()

    top_tickers = ohlcv.nlargest(200, "거래대금").index.tolist()
    use_days = min(days, 5)

    rows = []
    for i, ticker in enumerate(top_tickers):
        try:
            data = _fetch_stock_integration(ticker)
            trends = data.get("dealTrendInfos", [])
            if not trends:
                continue

            recent = trends[:use_days]
            inst_val, frgn_val, indv_val = 0, 0, 0
            for t in recent:
                price = _to_int(t.get("closePrice", 0)) or 1
                inst_val += _to_signed_int(t.get("organPureBuyQuant", 0)) * price
                frgn_val += _to_signed_int(t.get("foreignerPureBuyQuant", 0)) * price
                indv_val += _to_signed_int(t.get("individualPureBuyQuant", 0)) * price

            rows.append({
                "티커": ticker,
                "기관합계": inst_val,
                "외국인합계": frgn_val,
                "개인": indv_val,
            })
        except Exception:
            continue
        # 속도 조절
        if (i + 1) % 50 == 0:
            time.sleep(0.3)
        else:
            time.sleep(0.03)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).set_index("티커")


def get_investor_trend_individual(ticker: str) -> pd.DataFrame:
    """
    개별 종목 투자자 동향 (최근 5거래일, integration 엔드포인트).
    반환: index=날짜, columns=[기관합계, 외국인합계, 개인]  (순매수대금, 원)

    장중에는 오늘 closePrice=0 → Naver basic 엔드포인트로 현재가 보완.
    """
    data = _fetch_stock_integration(ticker)
    trends = data.get("dealTrendInfos", [])
    if not trends:
        return pd.DataFrame()

    today_str = datetime.datetime.now().strftime("%Y%m%d")
    _current_price_cache = {}  # 이 호출 내에서만 사용

    def _get_current_price() -> int:
        """Naver basic 엔드포인트로 현재가 조회 (호출당 1회 캐시)."""
        if "p" not in _current_price_cache:
            try:
                r = _session.get(f"{NAVER_API}/stock/{ticker}/basic", timeout=5)
                r.raise_for_status()
                _current_price_cache["p"] = _to_int(r.json().get("closePrice", 0))
            except Exception:
                _current_price_cache["p"] = 0
        return _current_price_cache["p"]

    rows = []
    for t in trends:
        bizdate = t.get("bizdate", "")
        if not bizdate:
            continue
        close = _to_int(t.get("closePrice", 0))
        # 장중 오늘 데이터는 closePrice=0 → 현재가로 대체
        if close == 0 and bizdate == today_str:
            close = _get_current_price()
        price = close or 1
        rows.append({
            "날짜": bizdate,
            "기관합계": _to_signed_int(t.get("organPureBuyQuant", 0)) * price,
            "외국인합계": _to_signed_int(t.get("foreignerPureBuyQuant", 0)) * price,
            "개인": _to_signed_int(t.get("individualPureBuyQuant", 0)) * price,
        })
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["날짜"] = pd.to_datetime(df["날짜"])
    df = df.set_index("날짜").sort_index()
    return df


# ---------------------------------------------------------------------------
# 공개 API : 업종(섹터) 정보
# ---------------------------------------------------------------------------

def get_sector_info(date: str, market: str = "ALL") -> pd.DataFrame:
    """
    전종목 업종(섹터) 매핑 — Naver Finance 업종별 시세 페이지 스크래핑.
    반환: index=티커, columns=[업종]
    """
    cache_key = f"sectors_{market}"
    if cache_key in _cache:
        cached = _cache[cache_key]
        if isinstance(cached, pd.DataFrame):
            return cached

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("[WARN] beautifulsoup4 미설치 — 섹터 정보를 가져올 수 없습니다.")
        return pd.DataFrame()

    # 1) 업종 목록 가져오기
    url = f"{NAVER_FINANCE}/sise/sise_group.naver"
    try:
        resp = _session.get(url, params={"type": "upjong"}, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        sector_links = soup.select("a[href*='sise_group_detail.naver?type=upjong']")
        sectors = []
        seen = set()
        for link in sector_links:
            href = link.get("href", "")
            m = re.search(r"no=(\d+)", href)
            if m:
                sno = m.group(1)
                sname = link.get_text(strip=True)
                if sname and sno not in seen:
                    sectors.append((sno, sname))
                    seen.add(sno)
    except Exception as e:
        print(f"[WARN] 업종 목록 스크래핑 실패: {e}")
        return pd.DataFrame()

    # (선택) 해당 시장 종목만 필터
    valid_tickers = None
    if market != "ALL":
        tdf = get_all_tickers(date, market)
        valid_tickers = set(tdf["티커"].tolist())

    # 2) 각 업종 구성 종목 수집
    rows = []
    for sno, sname in sectors:
        try:
            detail_url = f"{NAVER_FINANCE}/sise/sise_group_detail.naver"
            dr = _session.get(detail_url, params={"type": "upjong", "no": sno}, timeout=10)
            dr.raise_for_status()
            dsoup = BeautifulSoup(dr.text, "html.parser")
            for sl in dsoup.select("a[href*='/item/main.naver?code=']"):
                cm = re.search(r"code=(\d{6})", sl.get("href", ""))
                if cm:
                    tk = cm.group(1)
                    if valid_tickers is None or tk in valid_tickers:
                        rows.append({"티커": tk, "업종": sname})
        except Exception:
            continue
        time.sleep(0.05)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).drop_duplicates(subset="티커", keep="first").set_index("티커")
    _cache[cache_key] = df
    return df


# ---------------------------------------------------------------------------
# 공개 API : KOSPI / KOSDAQ 지수 시세
# ---------------------------------------------------------------------------

def get_index_ohlcv(index_code: str = "KOSPI", count: int = 120) -> pd.DataFrame:
    """
    KOSPI 또는 KOSDAQ 종합지수 일봉 히스토리.
    반환: index=날짜(DatetimeIndex), columns=[시가, 고가, 저가, 종가, 거래량, 등락률]
    """
    cache_key = f"index_ohlcv_{index_code}_{count}"
    if cache_key in _cache:
        cached = _cache[cache_key]
        if isinstance(cached, pd.DataFrame) and not cached.empty:
            return cached

    all_data: List[Dict] = []
    page = 1
    page_size = min(count, 60)
    while len(all_data) < count:
        url = f"{NAVER_API}/index/{index_code}/price"
        params = {"pageSize": page_size, "page": page}
        try:
            resp = _session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            items = resp.json()
        except Exception:
            break
        if not isinstance(items, list) or not items:
            break
        all_data.extend(items)
        if len(items) < page_size:
            break
        page += 1
        time.sleep(0.1)

    if not all_data:
        return pd.DataFrame()

    rows = []
    for d in all_data[:count]:
        date_val = d.get("localTradedAt", "") or d.get("날짜", "")
        if not date_val:
            continue
        rows.append({
            "날짜": date_val,
            "시가": _to_float(d.get("openPrice", 0)),
            "고가": _to_float(d.get("highPrice", 0)),
            "저가": _to_float(d.get("lowPrice", 0)),
            "종가": _to_float(d.get("closePrice", 0)),
            "거래량": _to_int(d.get("accumulatedTradingVolume", 0)),
            "등락률": _to_float(d.get("fluctuationsRatio", 0)),
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["날짜"] = pd.to_datetime(df["날짜"])
    df = df.set_index("날짜").sort_index()
    _cache[cache_key] = df
    return df


# ---------------------------------------------------------------------------
# 장중 시장 전체 투자자 수급 (실시간 - KOSPI/KOSDAQ 합산)
# ---------------------------------------------------------------------------

def get_market_investor_trend() -> Dict:
    """
    KOSPI + KOSDAQ 시장 전체 기관/외국인/개인 순매매 (장중 실시간).
    반환: {
        "bizdate": "20260316",
        "kospi": {"personal": +5957, "foreign": -3327, "institution": -2678},
        "kosdaq": {"personal": +3652, "foreign": -3135, "institution": -240},
        "total": {"personal": +9609, "foreign": -6462, "institution": -2918},
        "is_today": True
    }
    """
    cache_key = "market_investor_trend"
    now = time.time()
    cached = _cache.get(cache_key)
    if cached and now - cached.get("_ts", 0) < 60:  # 1분 캐시
        return cached

    result = {"bizdate": "", "kospi": {}, "kosdaq": {}, "total": {}, "is_today": False}
    today_str = datetime.datetime.now().strftime("%Y%m%d")

    def _parse_val(s: str) -> float:
        """'+5,957' → 5957.0  /  '-3,327' → -3327.0 (단위: 억원)"""
        if not s:
            return 0.0
        sign = -1 if s.startswith("-") else 1
        num = re.sub(r"[^0-9.]", "", s)
        return sign * float(num) if num else 0.0

    totals = {"personal": 0.0, "foreign": 0.0, "institution": 0.0}

    for idx_code, key in [("KOSPI", "kospi"), ("KOSDAQ", "kosdaq")]:
        url = f"{NAVER_API}/index/{idx_code}/trend"
        try:
            resp = _session.get(url, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list) or not data:
                continue
            latest = data[0]
            biz = latest.get("bizdate", "")
            if not result["bizdate"]:
                result["bizdate"] = biz
                result["is_today"] = (biz == today_str)

            vals = {
                "personal": _parse_val(latest.get("personalValue", "0")),
                "foreign": _parse_val(latest.get("foreignValue", "0")),
                "institution": _parse_val(latest.get("institutionalValue", "0")),
            }
            result[key] = vals
            for k in totals:
                totals[k] += vals[k]
        except Exception:
            continue

    result["total"] = totals
    result["_ts"] = now
    _cache[cache_key] = result
    return result


def clear_market_investor_cache():
    """장중 수급 캐시 초기화."""
    _cache.pop("market_investor_trend", None)


# ---------------------------------------------------------------------------
# KIS (한국투자증권) Open API — 개별 종목 장중 실시간 수급
#
# [설정 방법 — 우선순위]
# 1) 앱 UI에서 직접 입력 → portfolio_data/kis_credentials.json 저장 (권장)
# 2) Streamlit Cloud Secrets: [kis] app_key / app_secret
# 3) 환경변수: KIS_APP_KEY / KIS_APP_SECRET
# ---------------------------------------------------------------------------

KIS_API_BASE = "https://openapi.koreainvestment.com:9443"
_KIS_CRED_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "portfolio_data", "kis_credentials.json"
)


def save_kis_credentials(app_key: str, app_secret: str):
    """앱 UI에서 입력한 KIS 자격증명을 파일에 저장."""
    cred_path = os.path.normpath(_KIS_CRED_FILE)
    os.makedirs(os.path.dirname(cred_path), exist_ok=True)
    with open(cred_path, "w", encoding="utf-8") as f:
        json.dump({"app_key": app_key, "app_secret": app_secret}, f)
    # 토큰 캐시 무효화
    _cache.pop("_kis_tok", None)
    _cache.pop("_kis_tok_ts", None)


def delete_kis_credentials():
    """저장된 KIS 자격증명 삭제."""
    cred_path = os.path.normpath(_KIS_CRED_FILE)
    if os.path.exists(cred_path):
        os.remove(cred_path)
    for k in ["_kis_tok", "_kis_tok_ts"]:
        _cache.pop(k, None)


def _get_kis_credentials():
    """KIS 자격증명 획득 (우선순위: 파일 > st.secrets > 환경변수)."""
    # 1) 파일 기반 (앱 UI 입력)
    cred_path = os.path.normpath(_KIS_CRED_FILE)
    if os.path.exists(cred_path):
        try:
            with open(cred_path, "r", encoding="utf-8") as f:
                d = json.load(f)
            k, s = d.get("app_key", ""), d.get("app_secret", "")
            if k and s:
                return k, s
        except Exception:
            pass
    # 2) st.secrets
    try:
        import streamlit as st
        if hasattr(st, "secrets") and "kis" in st.secrets:
            return (
                str(st.secrets["kis"].get("app_key", "")),
                str(st.secrets["kis"].get("app_secret", "")),
            )
    except Exception:
        pass
    # 3) 환경변수
    return os.getenv("KIS_APP_KEY", ""), os.getenv("KIS_APP_SECRET", "")


def is_kis_configured() -> bool:
    """KIS API 자격증명 설정 여부."""
    k, s = _get_kis_credentials()
    return bool(k and s)


def get_kis_access_token() -> str:
    """
    KIS OAuth access token 발급 (23시간 캐시).
    자격증명 미설정 또는 실패 시 빈 문자열 반환.
    """
    cache_key, ts_key = "_kis_tok", "_kis_tok_ts"
    now = time.time()
    tok = _cache.get(cache_key, "")
    if tok and now - _cache.get(ts_key, 0) < 82800:  # 23시간
        return tok
    k, s = _get_kis_credentials()
    if not k or not s:
        return ""
    try:
        resp = requests.post(
            f"{KIS_API_BASE}/oauth2/tokenP",
            json={"grant_type": "client_credentials", "appkey": k, "appsecret": s},
            timeout=10,
        )
        resp.raise_for_status()
        tok = resp.json().get("access_token", "")
        if tok:
            _cache[cache_key] = tok
            _cache[ts_key] = now
        return tok
    except Exception:
        return ""


def get_kis_stock_investor(ticker: str) -> Dict:
    """
    KIS TR FHKST01010900: 주식현재가 투자자
    당일 기관/외국인/개인 순매수 거래대금 (장중 실시간, 1분 캐시).

    반환: {"외국인": -1.07, "기관": -4.51, "개인": 5.58}  (단위: 억원)
          자격증명 미설정 / API 오류 시: {} 반환

    [KIS API 응답 포맷 처리]
    - 포맷 A: output 이 단일 dict, 키= frgn_ntby_tr_pbmn / orgn_ntby_tr_pbmn / indv_ntby_tr_pbmn
    - 포맷 B: output 이 투자자유형별 리스트 (0=개인, 1=외국인, 2=기관합계), 각 행 키= ntby_tr_pbmn
    """
    if not is_kis_configured():
        return {}
    cache_key = f"kis_inv_{ticker}"
    now = time.time()
    cached = _cache.get(cache_key)
    if cached and now - cached.get("_ts", 0) < 60:  # 1분 캐시
        return {k: v for k, v in cached.items() if not k.startswith("_")}

    tok = get_kis_access_token()
    if not tok:
        _cache["_kis_inv_last_err"] = "토큰 발급 실패 (key/secret 확인 필요)"
        return {}
    app_key, app_secret = _get_kis_credentials()
    try:
        resp = requests.get(
            f"{KIS_API_BASE}/uapi/domestic-stock/v1/quotations/inquire-investor",
            headers={
                "Authorization": f"Bearer {tok}",
                "appkey": app_key,
                "appsecret": app_secret,
                "tr_id": "FHKST01010900",
                "custtype": "P",
            },
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("rt_cd") != "0":
            _cache["_kis_inv_last_err"] = f"rt_cd={data.get('rt_cd')} msg={data.get('msg1', '')}"
            return {}

        def _eok_str(s) -> float:
            """'+1,234,567,890' 또는 '-...' → 억원 float. 부호 보존."""
            v = str(s).replace(",", "").replace("+", "").strip()
            try:
                return float(v) / 1e8
            except ValueError:
                return 0.0

        # FHKST01010900: output = 30일치 리스트 (날짜 오름/내림 모두 가능)
        raw_output = data.get("output")
        raw_output1 = data.get("output1")
        raw_output2 = data.get("output2")

        # 진단: 실제 응답 구조를 캐시에 기록 (키 전체 표시)
        diag = {}
        if isinstance(raw_output, dict):
            diag["output_keys"] = list(raw_output.keys())
        elif isinstance(raw_output, list) and raw_output:
            diag["output_keys"] = f"list[{len(raw_output)}] row0={list(raw_output[0].keys())}"
        if isinstance(raw_output1, dict):
            diag["output1_keys"] = list(raw_output1.keys())
        elif isinstance(raw_output1, list) and raw_output1:
            diag["output1_keys"] = f"list[{len(raw_output1)}] row0={list(raw_output1[0].keys())}"
        _cache["_kis_inv_last_diag"] = str(diag)

        def _try_parse_dict(d: dict) -> tuple:
            """dict에서 외국인/기관/개인 순매수 거래대금 추출.
            tr_pbmn이 모두 0이면 qty × stck_clpr 로 금액 추정."""
            def _s(v) -> str:
                return str(v).replace(",", "").strip() if v is not None else "0"

            frgn = _eok_str(_s(d.get("frgn_ntby_tr_pbmn") or d.get("frgn_ntby_pbmn") or "0"))
            orgn = _eok_str(_s(d.get("orgn_ntby_tr_pbmn") or d.get("orgn_ntby_pbmn") or
                               d.get("ivtr_ntby_tr_pbmn") or "0"))
            indv = _eok_str(_s(d.get("prsn_ntby_tr_pbmn") or d.get("indv_ntby_tr_pbmn") or
                               d.get("prsn_ntby_pbmn") or "0"))

            # tr_pbmn이 모두 0이면 qty × 종가로 추정
            if frgn == 0.0 and orgn == 0.0:
                try:
                    price = float(_s(d.get("stck_clpr", "0")))
                    if price > 0:
                        fq = float(_s(d.get("frgn_ntby_qty", "0")))
                        oq = float(_s(d.get("orgn_ntby_qty", "0")))
                        pq = float(_s(d.get("prsn_ntby_qty", "0")))
                        if fq != 0.0 or oq != 0.0:
                            frgn = fq * price / 1e8
                            orgn = oq * price / 1e8
                            indv = pq * price / 1e8
                except Exception:
                    pass
            return frgn, orgn, indv

        def _row_has_data(d: dict) -> bool:
            """행에 유효한 수급 데이터가 있는지 확인 (모두 0/빈값이면 False)."""
            def _nz(v) -> bool:
                try:
                    return float(str(v).replace(",", "").strip()) != 0.0
                except Exception:
                    return False
            fields = ("frgn_ntby_tr_pbmn", "orgn_ntby_tr_pbmn", "prsn_ntby_tr_pbmn",
                      "frgn_ntby_qty", "orgn_ntby_qty", "prsn_ntby_qty")
            return any(_nz(d.get(f)) for f in fields)

        # ── output1 (단일 dict, 당일 현황) 우선 시도 ─────────────────
        if isinstance(raw_output1, dict) and raw_output1:
            frgn, orgn, indv = _try_parse_dict(raw_output1)
            result = {"외국인": frgn, "기관": orgn, "개인": indv, "_ts": now}
            _cache[cache_key] = result
            return {k: v for k, v in result.items() if not k.startswith("_")}

        # ── output (단일 dict) ────────────────────────────────────────
        if isinstance(raw_output, dict) and raw_output:
            frgn, orgn, indv = _try_parse_dict(raw_output)
            result = {"외국인": frgn, "기관": orgn, "개인": indv, "_ts": now}
            _cache[cache_key] = result
            return {k: v for k, v in result.items() if not k.startswith("_")}

        # ── 리스트: 최근 날짜부터 순서대로 시도, 유효 데이터 있는 행 선택 ──
        for candidate in [raw_output, raw_output1, raw_output2]:
            if not isinstance(candidate, list) or not candidate:
                continue
            rows = [r for r in candidate if isinstance(r, dict)]
            if not rows:
                continue

            # stck_bsop_date 기준 DESC 정렬 → 최근 날짜가 rows[0]
            if any(r.get("stck_bsop_date") for r in rows):
                rows = sorted(rows, key=lambda r: r.get("stck_bsop_date", ""), reverse=True)

            # 통합 행(1행=1일, 모든 투자자 포함) 구조인지 확인
            has_combined = any(
                rows[0].get(k) is not None
                for k in ("frgn_ntby_tr_pbmn", "orgn_ntby_tr_pbmn",
                          "frgn_ntby_qty", "orgn_ntby_qty")
            )
            if has_combined:
                # 최신 날짜부터 유효 데이터가 있는 행 선택 (오늘 row가 0이면 전일로 폴백)
                target = None
                target_date = ""
                for row in rows:
                    if _row_has_data(row):
                        target = row
                        target_date = row.get("stck_bsop_date", "")
                        break
                if target is None:
                    target = rows[0]  # 모두 0이면 최신 행 사용
                frgn, orgn, indv = _try_parse_dict(target)
                # 진단에 실제 사용된 날짜와 값 기록
                diag["used_date"] = target_date
                diag["parsed"] = f"frgn={frgn:.2f} orgn={orgn:.2f} indv={indv:.2f}"
                _cache["_kis_inv_last_diag"] = str(diag)
                result = {"외국인": frgn, "기관": orgn, "개인": indv, "_ts": now}
                _cache[cache_key] = result
                _cache.pop("_kis_inv_last_err", None)
                return {k: v for k, v in result.items() if not k.startswith("_")}

            # 유형별 행 리스트: row[0]=개인 row[1]=외국인 row[2]=기관합계
            if len(rows) >= 3 and "ntby_tr_pbmn" in rows[0]:
                result = {
                    "개인": _eok_str(rows[0].get("ntby_tr_pbmn", "0")),
                    "외국인": _eok_str(rows[1].get("ntby_tr_pbmn", "0")),
                    "기관": _eok_str(rows[2].get("ntby_tr_pbmn", "0")),
                    "_ts": now,
                }
                _cache[cache_key] = result
                _cache.pop("_kis_inv_last_err", None)
                return {k: v for k, v in result.items() if not k.startswith("_")}

        _cache["_kis_inv_last_err"] = f"파싱 실패 diag={diag}"
        return {}
    except Exception as e:
        _cache["_kis_inv_last_err"] = str(e)[:120]
        return {}


def get_kis_investor_last_error() -> str:
    """마지막 KIS 투자자 TR 에러 메시지 반환 (진단용)."""
    return _cache.get("_kis_inv_last_err", "")


def get_kis_investor_last_diag() -> str:
    """마지막 KIS 투자자 TR 응답 구조 진단 정보 반환."""
    return _cache.get("_kis_inv_last_diag", "")


def clear_kis_investor_cache(ticker: str = None):
    """KIS 투자자 수급 캐시 초기화."""
    if ticker:
        _cache.pop(f"kis_inv_{ticker}", None)
    else:
        for k in [k for k in list(_cache) if k.startswith("kis_inv_")]:
            _cache.pop(k, None)


def get_kis_realtime_price(ticker: str) -> dict:
    """
    KIS TR FHKST01010100: 주식현재가 시세
    현재가 / 전일대비 등락률 실시간 조회 (30초 캐시).
    반환: {"price": int, "change_rate": float} 또는 {}
    """
    if not is_kis_configured():
        return {}
    cache_key = f"kis_price_{ticker}"
    now = time.time()
    cached = _cache.get(cache_key)
    if cached and now - cached.get("_ts", 0) < 30:
        return {k: v for k, v in cached.items() if not k.startswith("_")}

    tok = get_kis_access_token()
    if not tok:
        return {}
    app_key, app_secret = _get_kis_credentials()
    try:
        resp = requests.get(
            f"{KIS_API_BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers={
                "Authorization": f"Bearer {tok}",
                "appkey": app_key,
                "appsecret": app_secret,
                "tr_id": "FHKST01010100",
                "custtype": "P",
            },
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("rt_cd") != "0":
            return {}
        item = data.get("output", {})
        price = int(str(item.get("stck_prpr", "0")).replace(",", "") or 0)
        rate_str = str(item.get("prdy_ctrt", "0")).replace("+", "").replace(",", "")
        try:
            rate = float(rate_str)
        except ValueError:
            rate = 0.0
        result = {"price": price, "change_rate": rate, "_ts": now}
        _cache[cache_key] = result
        return {k: v for k, v in result.items() if not k.startswith("_")}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# 종합 데이터 빌더 (메인 파이프라인)
# ---------------------------------------------------------------------------

def build_daily_dataset(date=None, market: str = "ALL") -> pd.DataFrame:
    """전종목 시세 + 수급 + 업종 정보 결합."""
    if date is None:
        date = get_latest_trading_date()

    ohlcv = get_market_ohlcv(date, market)
    if ohlcv.empty:
        return pd.DataFrame()

    tickers_df = get_all_tickers(date, market).set_index("티커")
    ohlcv = ohlcv.join(tickers_df[["종목명"]], how="left")

    supply = get_accumulated_investor_trading(date, days=5, market=market)
    if not supply.empty:
        supply.columns = [f"{c}_5일" for c in supply.columns]
        ohlcv = ohlcv.join(supply, how="left")

    sector = get_sector_info(date, market)
    if not sector.empty:
        ohlcv = ohlcv.join(sector, how="left")

    fill_cols = [c for c in ohlcv.columns if "5일" in c]
    ohlcv[fill_cols] = ohlcv[fill_cols].fillna(0)

    return ohlcv


# ---------------------------------------------------------------------------
# 공개 API : 펀더멘털 (PER, EPS, 분기 실적)
# ---------------------------------------------------------------------------

def get_stock_fundamentals(ticker: str) -> Dict:
    """
    종목의 PER, EPS, 분기별 영업이익 등 펀더멘털 정보.
    integration + finance/quarter 엔드포인트를 결합.
    """
    result = {"PER": "", "EPS": "", "PBR": "", "BPS": "", "배당수익률": "", "분기실적": []}

    def _strip_unit(s: str) -> str:
        """'3.06배' → '3.06', '6,564원' → '6564', '0.85%' → '0.85' 등 단위 제거."""
        return str(s).replace(",", "").replace("배", "").replace("원", "").replace("%", "").strip()

    # 1) integration 에서 PER/EPS/PBR
    data = _fetch_stock_integration(ticker)
    for info in data.get("totalInfos", []):
        code = info.get("code", "")
        val = _strip_unit(str(info.get("value", "")))
        if code == "per":
            result["PER"] = val
        elif code == "eps":
            result["EPS"] = val
        elif code == "pbr":
            result["PBR"] = val
        elif code == "bps":
            result["BPS"] = val
        elif code == "dividendYieldRatio":
            result["배당수익률"] = val

    # 2) 분기 실적
    cache_key = f"quarter_{ticker}"
    if cache_key in _cache:
        result["분기실적"] = _cache[cache_key]
        return result

    try:
        url = f"{NAVER_API}/stock/{ticker}/finance/quarter"
        resp = _session.get(url, timeout=10)
        resp.raise_for_status()
        qdata = resp.json()

        fi = qdata.get("financeInfo", {})
        titles = fi.get("trTitleList", [])
        rows_list = fi.get("rowList", [])

        # 영업이익 행 찾기
        op_profit_row = None
        revenue_row = None
        for row in rows_list:
            if row.get("title") == "영업이익":
                op_profit_row = row
            elif row.get("title") == "매출액":
                revenue_row = row

        quarters = []
        for t in titles:
            key = t.get("key", "")
            label = t.get("title", "")
            is_est = t.get("isConsensus", "N") == "Y"
            op_val = ""
            rev_val = ""
            if op_profit_row:
                col = op_profit_row.get("columns", {}).get(key, {})
                op_val = col.get("value", "") if col else ""
            if revenue_row:
                col = revenue_row.get("columns", {}).get(key, {})
                rev_val = col.get("value", "") if col else ""
            quarters.append({
                "분기": label,
                "매출액": rev_val,
                "영업이익": op_val,
                "추정": is_est,
            })

        result["분기실적"] = quarters
        _cache[cache_key] = quarters
    except Exception:
        pass

    return result


def get_weekly_volume_trend(ticker: str) -> Dict:
    """
    최근 7거래일 거래량 추이 + 가격 추세 판단.
    반환: {
        "volumes": [int...],  # 최근 7일 거래량 (오래된 순)
        "prices": [int...],   # 최근 7일 종가
        "vol_trend": "증가" | "감소" | "보합",
        "price_trend": "상승" | "하락" | "보합",
        "avg_vol_5d": int,
    }
    """
    items = _fetch_stock_price_history(ticker, count=10)
    if not items or len(items) < 3:
        return {"volumes": [], "prices": [], "vol_trend": "N/A",
                "price_trend": "N/A", "avg_vol_5d": 0}

    # 최신이 먼저이므로 역순
    recent = list(reversed(items[:7]))
    vols = [_to_int(d.get("accumulatedTradingVolume", 0)) for d in recent]
    prices = [_to_int(d.get("closePrice", 0)) for d in recent]

    # 거래량 추세: 후반 3일 평균 vs 전반 3일 평균
    if len(vols) >= 6:
        first_half = np.mean(vols[:3])
        second_half = np.mean(vols[-3:])
        if first_half > 0:
            ratio = second_half / first_half
            vol_trend = "증가" if ratio > 1.2 else ("감소" if ratio < 0.8 else "보합")
        else:
            vol_trend = "보합"
    else:
        vol_trend = "보합"

    # 가격 추세
    if len(prices) >= 3 and prices[0] > 0:
        price_change = (prices[-1] - prices[0]) / prices[0]
        price_trend = "상승" if price_change > 0.01 else ("하락" if price_change < -0.01 else "보합")
    else:
        price_trend = "보합"

    avg_vol_5d = int(np.mean(vols[-5:])) if len(vols) >= 5 else int(np.mean(vols))

    return {
        "volumes": vols,
        "prices": prices,
        "vol_trend": vol_trend,
        "price_trend": price_trend,
        "avg_vol_5d": avg_vol_5d,
    }


# ---------------------------------------------------------------------------
# 미국 주식 (yfinance 기반)
# ---------------------------------------------------------------------------

def get_us_stock_ohlcv(ticker: str, period: str = "6mo") -> pd.DataFrame:
    """미국 개별 종목 OHLCV 히스토리."""
    try:
        import yfinance as yf
        data = yf.download(ticker, period=period, progress=False)
        return data
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# 일일 CSV 스냅샷 — 수급 데이터 누적 저장 (5일 제한 극복)
# ---------------------------------------------------------------------------

_SNAPSHOT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "snapshots")


def _is_market_closed(date: str) -> bool:
    """주어진 날짜의 장이 마감됐는지 판단. 과거면 True, 오늘이면 16시 이후 True."""
    today = datetime.date.today()
    d = datetime.datetime.strptime(date, "%Y%m%d").date()
    if d < today:
        return True
    if d == today and datetime.datetime.now().hour >= 16:
        return True
    return False


def smart_load_daily_data(date: str, market: str = "ALL", supply_days: int = 5) -> pd.DataFrame:
    """
    스마트 데이터 로더:
    1) 장 마감된 날짜 → 스냅샷 CSV가 있으면 즉시 로드 (~0.5초)
    2) 스냅샷 없거나 장중 → API에서 fetch 후 장 마감이면 스냅샷 저장
    3) 등락률이 전부 0(장 외 시간) → 개별 종목 히스토리로 보정
    """
    # 1) 스냅샷 체크 (장 마감된 날짜만)
    if _is_market_closed(date):
        cached = load_daily_snapshot(date, market)
        if not cached.empty and "종목명" in cached.columns:
            # 등락률이 전부 0인 비정상 스냅샷이면 무시하고 API에서 다시 fetch
            if "등락률" in cached.columns and (cached["등락률"] != 0).any():
                return cached

    # 2) API에서 fetch
    ohlcv = get_market_ohlcv(date, market)
    if ohlcv.empty:
        return pd.DataFrame()

    tickers_df = get_all_tickers(date, market).set_index("티커")
    ohlcv = ohlcv.join(tickers_df[["종목명"]], how="left")

    supply = get_accumulated_investor_trading(date, days=supply_days, market=market)
    if not supply.empty:
        supply.columns = [f"{c}_5일" for c in supply.columns]
        ohlcv = ohlcv.join(supply, how="left")

    sectors = get_sector_info(date, market)
    if not sectors.empty:
        ohlcv = ohlcv.join(sectors, how="left")

    fill_cols = [c for c in ohlcv.columns if "5일" in c]
    ohlcv[fill_cols] = ohlcv[fill_cols].fillna(0)

    # 3) 등락률이 전부 0이면 (장 외 시간) → 개별 종목 히스토리로 보정
    if "등락률" in ohlcv.columns and not (ohlcv["등락률"] != 0).any():
        ohlcv = _patch_change_rates_from_history(ohlcv, date)

    # 4) 장 마감됐으면 스냅샷 저장 (다음 조회부터 초고속 로드)
    #    단, 등락률이 전부 0이면 장 외 시간 비정상 데이터이므로 저장 안 함
    if _is_market_closed(date):
        if "등락률" in ohlcv.columns and (ohlcv["등락률"] != 0).any():
            _save_full_snapshot(ohlcv, date, market)

    return ohlcv


def _patch_change_rates_from_history(ohlcv: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    장 외 시간에 등락률이 모두 0일 때, 개별 종목 price 히스토리 API로 보정.
    시가총액 상위 500종목의 등락률을 업데이트하고, 나머지는 0으로 둔다.
    """
    import logging
    logger = logging.getLogger(__name__)
    logger.info("[Fetcher] 장 외 시간 등락률 보정 시작 (히스토리 API)")

    # 시가총액 기준 상위 500종목만 보정 (API 부하 제한)
    if "시가총액" in ohlcv.columns:
        targets = ohlcv.nlargest(500, "시가총액").index.tolist()
    else:
        targets = ohlcv.head(500).index.tolist()

    patched = 0
    for i, ticker in enumerate(targets):
        try:
            items = _fetch_stock_price_history(ticker, count=2)
            if not items:
                continue

            # 가장 최근 거래일 데이터
            latest = items[0]
            frate = _to_float(latest.get("fluctuationsRatio", 0))
            close_price = _to_int(latest.get("closePrice", 0))

            if frate != 0 and ticker in ohlcv.index:
                ohlcv.at[ticker, "등락률"] = frate
                # OHLCV도 정확한 값으로 보정
                ohlcv.at[ticker, "종가"] = close_price
                ohlcv.at[ticker, "고가"] = _to_int(latest.get("highPrice", close_price))
                ohlcv.at[ticker, "저가"] = _to_int(latest.get("lowPrice", close_price))
                ohlcv.at[ticker, "시가"] = _to_int(latest.get("openPrice", close_price))
                vol = _to_int(latest.get("accumulatedTradingVolume", 0))
                if vol > 0:
                    ohlcv.at[ticker, "거래량"] = vol
                patched += 1
        except Exception:
            continue

        # API 부하 제한 (50건마다 0.3초 휴식)
        if (i + 1) % 50 == 0:
            time.sleep(0.3)
        else:
            time.sleep(0.02)

    logger.info(f"[Fetcher] 등락률 보정 완료: {patched}/{len(targets)}종목")
    return ohlcv


def _save_full_snapshot(df: pd.DataFrame, date: str, market: str):
    """종목명 + 수급 + 섹터 포함 완전한 스냅샷 저장."""
    os.makedirs(_SNAPSHOT_DIR, exist_ok=True)
    filepath = os.path.join(_SNAPSHOT_DIR, f"{date}_{market}.csv")
    df["스냅샷일자"] = date
    df.to_csv(filepath, encoding="utf-8-sig")



def save_daily_snapshot(date: str, market: str = "ALL") -> str:
    """
    당일 전종목 시세 + 수급 스냅샷을 CSV로 저장.
    매일 한 번 실행하면 장기 수급 흐름 추적이 가능합니다.

    저장 위치: snapshots/YYYYMMDD_{market}.csv
    반환: 저장된 파일 경로
    """
    os.makedirs(_SNAPSHOT_DIR, exist_ok=True)
    filepath = os.path.join(_SNAPSHOT_DIR, f"{date}_{market}.csv")

    # 이미 저장된 스냅샷이면 skip
    if os.path.exists(filepath):
        return filepath

    ohlcv = get_market_ohlcv(date, market)
    if ohlcv.empty:
        return ""

    tickers_df = get_all_tickers(date, market).set_index("티커")
    ohlcv = ohlcv.join(tickers_df[["종목명"]], how="left")

    supply = get_accumulated_investor_trading(date, days=5, market=market)
    if not supply.empty:
        supply.columns = [f"{c}_5일" for c in supply.columns]
        ohlcv = ohlcv.join(supply, how="left")

    ohlcv["스냅샷일자"] = date
    ohlcv.to_csv(filepath, encoding="utf-8-sig")
    return filepath


def load_daily_snapshot(date: str, market: str = "ALL") -> pd.DataFrame:
    """저장된 특정 일자 스냅샷 로드."""
    filepath = os.path.join(_SNAPSHOT_DIR, f"{date}_{market}.csv")
    if not os.path.exists(filepath):
        return pd.DataFrame()
    df = pd.read_csv(filepath, index_col=0, encoding="utf-8-sig")
    return df


def load_snapshot_range(start_date: str, end_date: str, market: str = "ALL") -> pd.DataFrame:
    """
    기간 내 저장된 모든 스냅샷을 병합하여 반환.
    반환: 멀티인덱스 (스냅샷일자, 티커) DataFrame
    """
    if not os.path.isdir(_SNAPSHOT_DIR):
        return pd.DataFrame()

    start = datetime.datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.datetime.strptime(end_date, "%Y%m%d").date()

    frames = []
    for fname in sorted(os.listdir(_SNAPSHOT_DIR)):
        if not fname.endswith(f"_{market}.csv"):
            continue
        date_part = fname.split("_")[0]
        try:
            fdate = datetime.datetime.strptime(date_part, "%Y%m%d").date()
        except ValueError:
            continue
        if start <= fdate <= end:
            fpath = os.path.join(_SNAPSHOT_DIR, fname)
            df = pd.read_csv(fpath, index_col=0, encoding="utf-8-sig")
            if "스냅샷일자" not in df.columns:
                df["스냅샷일자"] = date_part
            frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=False)


def list_available_snapshots(market: str = "ALL") -> list:
    """저장된 스냅샷 일자 목록 반환."""
    if not os.path.isdir(_SNAPSHOT_DIR):
        return []
    dates = []
    suffix = f"_{market}.csv"
    for fname in sorted(os.listdir(_SNAPSHOT_DIR)):
        if fname.endswith(suffix):
            dates.append(fname.replace(suffix, ""))
    return dates


# ---------------------------------------------------------------------------
# 테마 데이터 (Naver Finance HTML 스크래핑)
# ---------------------------------------------------------------------------

def get_theme_list() -> pd.DataFrame:
    """
    Naver Finance 테마별 시세 스크래핑.
    반환: columns=[테마명, 테마번호, 등락률, 종목수]
    """
    cache_key = "theme_list"
    if cache_key in _cache:
        cached = _cache[cache_key]
        if isinstance(cached, pd.DataFrame):
            return cached

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return pd.DataFrame()

    themes = []
    for page in range(1, 6):
        url = f"{NAVER_FINANCE}/sise/theme.naver"
        try:
            resp = _session.get(url, params={"page": page}, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            rows = soup.select("table.type_1 tr")
            for row in rows:
                cols = row.select("td")
                if len(cols) < 4:
                    continue
                link = row.select_one("a[href*='type=theme']")
                if not link:
                    continue
                theme_name = link.get_text(strip=True)
                href = link.get("href", "")
                m = re.search(r"no=(\d+)", href)
                theme_no = m.group(1) if m else ""

                # 등락률
                change_text = cols[1].get_text(strip=True).replace("%", "").replace("+", "")
                change_val = _to_float(change_text) if change_text else 0.0

                themes.append({
                    "테마명": theme_name,
                    "테마번호": theme_no,
                    "등락률": change_val,
                })

            if not rows or len([r for r in rows if r.select("td")]) < 5:
                break
        except Exception:
            break
        time.sleep(0.1)

    if not themes:
        return pd.DataFrame()
    df = pd.DataFrame(themes)
    _cache[cache_key] = df
    return df


def get_theme_constituents(theme_no: str) -> pd.DataFrame:
    """
    특정 테마 구성 종목 스크래핑.
    반환: columns=[티커, 종목명, 현재가, 등락률, 거래대금]
    """
    cache_key = f"theme_const_{theme_no}"
    if cache_key in _cache:
        cached = _cache[cache_key]
        if isinstance(cached, pd.DataFrame):
            return cached

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return pd.DataFrame()

    url = f"{NAVER_FINANCE}/sise/sise_group_detail.naver"
    try:
        resp = _session.get(url, params={"type": "theme", "no": theme_no}, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return pd.DataFrame()

    rows_data = []
    for link in soup.select("a[href*='/item/main.naver?code=']"):
        m = re.search(r"code=(\d{6})", link.get("href", ""))
        if not m:
            continue
        ticker = m.group(1)
        name = link.get_text(strip=True)
        if not name:
            continue

        tr = link.find_parent("tr")
        if not tr:
            continue
        tds = tr.select("td")
        # 테이블 구조: N(0) | 종목명(1) | 현재가(2) | 전일비(3) | 등락률(4) | 매수호가(5) | 매도호가(6) | 거래량(7) | 거래대금(8)
        price = _to_int(tds[2].get_text(strip=True)) if len(tds) > 2 else 0
        change_text = tds[4].get_text(strip=True).replace("%", "").replace("+", "") if len(tds) > 4 else "0"
        change_val = _to_float(change_text)
        tv = _to_int(tds[8].get_text(strip=True)) if len(tds) > 8 else 0

        rows_data.append({
            "티커": ticker,
            "종목명": name,
            "현재가": price,
            "등락률": change_val,
            "거래대금": tv,
        })

    if not rows_data:
        return pd.DataFrame()

    df = pd.DataFrame(rows_data)
    _cache[cache_key] = df
    return df
    time.sleep(0.1)


# ---------------------------------------------------------------------------
# 프로그램 매매 데이터
# ---------------------------------------------------------------------------

def get_program_trading_top() -> pd.DataFrame:
    """
    프로그램 매매 순매수/순매도 상위 종목 스크래핑.
    반환: columns=[티커, 종목명, 프로그램순매수(백만원), 유형(순매수/순매도)]
    """
    cache_key = "program_trading"
    if cache_key in _cache:
        cached = _cache[cache_key]
        if isinstance(cached, pd.DataFrame):
            return cached

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return pd.DataFrame()

    url = f"{NAVER_FINANCE}/sise/programTrade.naver"
    try:
        resp = _session.get(url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return pd.DataFrame()

    all_rows = []
    tables = soup.select("table.type_1")

    for tidx, table in enumerate(tables[:2]):
        trade_type = "순매수" if tidx == 0 else "순매도"
        for row in table.select("tr"):
            tds = row.select("td")
            if len(tds) < 4:
                continue
            link = row.select_one("a[href*='main.naver?code=']")
            if not link:
                continue
            m = re.search(r"code=(\d{6})", link.get("href", ""))
            if not m:
                continue
            ticker = m.group(1)
            name = link.get_text(strip=True)
            amount_text = tds[3].get_text(strip=True) if len(tds) > 3 else "0"
            amount = _to_signed_int(amount_text)

            all_rows.append({
                "티커": ticker,
                "종목명": name,
                "프로그램순매수": amount if trade_type == "순매수" else -abs(amount),
                "유형": trade_type,
            })

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    _cache[cache_key] = df
    return df


# ---------------------------------------------------------------------------
# 종목 뉴스
# ---------------------------------------------------------------------------

def get_stock_news_list(ticker: str, count: int = 10) -> List[Dict]:
    """
    종목별 최신 뉴스 리스트.
    반환: [{"title": str, "date": str, "source": str, "url": str}, ...]
    """
    cache_key = f"news_{ticker}"
    if cache_key in _cache:
        return _cache[cache_key]

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    url = f"{NAVER_FINANCE}/item/news_news.naver"
    try:
        resp = _session.get(url, params={"code": ticker, "page": 1}, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return []

    articles = []
    for row in soup.select("tr"):
        title_el = row.select_one("a.tit")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        href = title_el.get("href", "")
        if href and not href.startswith("http"):
            href = f"https://finance.naver.com{href}"
        info_el = row.select_one("td.info")
        source = info_el.get_text(strip=True) if info_el else ""
        date_el = row.select_one("td.date")
        date_text = date_el.get_text(strip=True) if date_el else ""

        articles.append({
            "title": title,
            "date": date_text,
            "source": source,
            "url": href,
        })
        if len(articles) >= count:
            break

    _cache[cache_key] = articles
    return articles


# ---------------------------------------------------------------------------
# 상한가 / 급등 / 급락 감지
# ---------------------------------------------------------------------------

def detect_limit_up_stocks(daily_df: pd.DataFrame, threshold: float = 29.0) -> pd.DataFrame:
    """상한가 근접 종목 (등락률 >= threshold%)."""
    if daily_df.empty or "등락률" not in daily_df.columns:
        return pd.DataFrame()
    mask = daily_df["등락률"] >= threshold
    return daily_df[mask].sort_values("등락률", ascending=False).copy()


def detect_sharp_drop_stocks(
    daily_df: pd.DataFrame, threshold: float = -10.0, sector_keyword: str = ""
) -> pd.DataFrame:
    """급락 종목 (등락률 <= threshold%). sector_keyword가 있으면 해당 업종만 필터."""
    if daily_df.empty or "등락률" not in daily_df.columns:
        return pd.DataFrame()
    mask = daily_df["등락률"] <= threshold
    result = daily_df[mask].sort_values("등락률", ascending=True).copy()
    if sector_keyword and "업종" in result.columns:
        result = result[result["업종"].str.contains(sector_keyword, na=False, case=False)]
    return result


def detect_volume_spike_stocks(daily_df: pd.DataFrame, min_change: float = 3.0) -> pd.DataFrame:
    """거래대금 기반 이상 급등 종목 (등락률 >= min_change%)."""
    if daily_df.empty:
        return pd.DataFrame()
    mask = (daily_df["등락률"] >= min_change) & (daily_df.get("거래대금", pd.Series(dtype=float)) > 0)
    result = daily_df[mask].sort_values("거래대금", ascending=False).copy()
    return result.head(50)
