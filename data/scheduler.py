"""
백그라운드 데이터 갱신 스케줄러

- 하루 8번 정시(9~16시)마다 최신 데이터를 자동 갱신
- 정시 정각(분=0)에만 갱신 (중복 방지)
- 평일(월~금)에만 동작
- 장중(09:00~16:00)에 API 호출, 장 마감 후에는 스냅샷 저장
- 스레드 안전한 데이터 저장소 제공
- 발굴/트레이더 탭 사전 분석 (smart_top3, 기술적 지표, 프로그램매매, 테마)
"""

import threading
import time
import logging
import datetime
from typing import Optional, List, Dict, Any

import pandas as pd

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
# 스레드 안전 데이터 저장소
# ─────────────────────────────────────────────────────────────────────

class _DataStore:
    """백그라운드에서 갱신한 최신 데이터를 스레드 안전하게 보관."""

    def __init__(self):
        self._lock = threading.Lock()
        self._daily_df: Optional[pd.DataFrame] = None
        self._date: str = ""
        self._market: str = ""
        self._updated_at: Optional[datetime.datetime] = None
        self._is_refreshing: bool = False
        self._refresh_count: int = 0

    def get(self) -> tuple:
        """(daily_df, date, market, updated_at) 반환."""
        with self._lock:
            return (
                self._daily_df.copy() if self._daily_df is not None else None,
                self._date,
                self._market,
                self._updated_at,
            )

    def put(self, daily_df: pd.DataFrame, date: str, market: str):
        """최신 데이터 저장."""
        with self._lock:
            self._daily_df = daily_df
            self._date = date
            self._market = market
            self._updated_at = datetime.datetime.now()
            self._refresh_count += 1

    @property
    def is_refreshing(self) -> bool:
        with self._lock:
            return self._is_refreshing

    @is_refreshing.setter
    def is_refreshing(self, val: bool):
        with self._lock:
            self._is_refreshing = val

    @property
    def refresh_count(self) -> int:
        with self._lock:
            return self._refresh_count

    def has_data(self, date: str = "", market: str = "") -> bool:
        """지정 날짜/시장의 데이터가 있는지 확인."""
        with self._lock:
            if self._daily_df is None or self._daily_df.empty:
                return False
            if date and self._date != date:
                return False
            if market and self._market != market:
                return False
            return True


class _AnalysisStore:
    """발굴/트레이더 탭용 사전 분석 결과를 스레드 안전하게 보관."""

    def __init__(self):
        self._lock = threading.Lock()
        self._smart_top3: List[Dict[str, Any]] = []
        self._screened_df: Optional[pd.DataFrame] = None
        self._program_trading: Optional[pd.DataFrame] = None
        self._theme_list: Optional[pd.DataFrame] = None
        self._date: str = ""
        self._updated_at: Optional[datetime.datetime] = None
        self._is_analyzing: bool = False

    def put_smart_top3(self, results: list, date: str):
        with self._lock:
            self._smart_top3 = results
            self._date = date
            self._updated_at = datetime.datetime.now()

    def get_smart_top3(self, date: str = "") -> list:
        with self._lock:
            if date and self._date != date:
                return []
            return list(self._smart_top3)

    def put_screened(self, df: pd.DataFrame, date: str):
        with self._lock:
            self._screened_df = df
            self._date = date

    def get_screened(self, date: str = "") -> Optional[pd.DataFrame]:
        with self._lock:
            if date and self._date != date:
                return None
            if self._screened_df is not None:
                return self._screened_df.copy()
            return None

    def put_program_trading(self, df: pd.DataFrame):
        with self._lock:
            self._program_trading = df

    def get_program_trading(self) -> Optional[pd.DataFrame]:
        with self._lock:
            if self._program_trading is not None:
                return self._program_trading.copy()
            return None

    def put_theme_list(self, df: pd.DataFrame):
        with self._lock:
            self._theme_list = df

    def get_theme_list(self) -> Optional[pd.DataFrame]:
        with self._lock:
            if self._theme_list is not None:
                return self._theme_list.copy()
            return None

    @property
    def is_analyzing(self) -> bool:
        with self._lock:
            return self._is_analyzing

    @is_analyzing.setter
    def is_analyzing(self, val: bool):
        with self._lock:
            self._is_analyzing = val

    @property
    def updated_at(self) -> Optional[datetime.datetime]:
        with self._lock:
            return self._updated_at

    def has_analysis(self, date: str = "") -> bool:
        with self._lock:
            if not self._smart_top3 and self._screened_df is None:
                return False
            if date and self._date != date:
                return False
            return True


# 글로벌 데이터 저장소 (Streamlit 프로세스 생존 주기 동안 유지)
_store = _DataStore()
_analysis = _AnalysisStore()


# ─────────────────────────────────────────────────────────────────────
# 스케줄러
# ─────────────────────────────────────────────────────────────────────

_scheduler_thread: Optional[threading.Thread] = None
_scheduler_stop = threading.Event()

# 설정

# 9:10에 첫 데이터 갱신을 위해 REFRESH_MINUTES를 도입
REFRESH_INTERVAL_SEC = 60  # 1분마다 체크 (정시 갱신 감지용)
MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 16
REFRESH_HOURS = [9, 10, 11, 12, 13, 14, 15, 16]  # 정시만 갱신
REFRESH_MINUTES = {9: 10}  # 9시는 10분에, 나머지는 0분에 갱신


def _is_market_hours() -> bool:
    """현재 장 운영 시간(09~16시, 평일)인지 확인."""
    now = datetime.datetime.now()
    if now.weekday() >= 5:  # 주말
        return False
    return MARKET_OPEN_HOUR <= now.hour < MARKET_CLOSE_HOUR


def _do_refresh(date: str, market: str, supply_days: int):
    """실제 데이터 갱신 수행 (백그라운드 스레드에서 호출)."""
    # 순환 import 방지: 함수 내 import
    from data.fetcher import smart_load_daily_data, _cache

    try:
        _store.is_refreshing = True
        logger.info(f"[Scheduler] 데이터 갱신 시작: {date} / {market}")

        # 인메모리 캐시 클리어 (전종목 시세 + 수급 integration)
        for k in [k for k in list(_cache)
                  if k.startswith("stocks_") or k.startswith("integration_")]:
            _cache.pop(k, None)

        daily_df = smart_load_daily_data(date, market, supply_days)
        # ※ smart_load_daily_data 내부에서 price_cache.update_from_dataframe() 자동 호출

        if not daily_df.empty:
            _store.put(daily_df, date, market)
            logger.info(
                f"[Scheduler] 갱신 완료: {len(daily_df)}종목, "
                f"{datetime.datetime.now().strftime('%H:%M:%S')}"
            )
        else:
            logger.warning("[Scheduler] 갱신 결과 비어있음")
    except Exception as e:
        logger.error(f"[Scheduler] 갱신 실패: {e}")
    finally:
        _store.is_refreshing = False


def _do_analysis(date: str):
    """daily_df 기반 발굴/트레이더 사전 분석 (백그라운드)."""
    try:
        _analysis.is_analyzing = True
        logger.info("[Scheduler] 사전 분석 시작 (발굴/트레이더)")

        df, stored_date, _, _ = _store.get()
        if df is None or df.empty:
            logger.warning("[Scheduler] 분석 스킵: daily_df 없음")
            return

        # ── 1) Smart Top 3: 수급 상위 30종목 점수 계산 ──
        _precompute_smart_top3(df, date)

        # ── 2) 기술적 지표 스크리닝 (수급 상위 100종목) ──
        _precompute_screened(df, date)

        # ── 3) 프로그램 매매 데이터 (트레이더 Type C) ──
        _precompute_program_trading()

        # ── 4) 테마 목록 (트레이더 Type A) ──
        _precompute_theme_list()

        # ── 5) 발굴 종목 price_cache 개별 갱신 (KIS 우선 → Naver)
        #       분석 완료 후 주요 발굴 티커를 최신 가격으로 추가 갱신합니다.
        #       장중에만 실행합니다 (장외에는 _do_refresh 의 bulk 갱신으로 충분).
        if _is_market_hours():
            _refresh_tracked_prices(date)

        logger.info(
            f"[Scheduler] 사전 분석 완료: "
            f"{datetime.datetime.now().strftime('%H:%M:%S')}"
        )
    except Exception as e:
        logger.error(f"[Scheduler] 사전 분석 실패: {e}")
    finally:
        _analysis.is_analyzing = False


def _refresh_tracked_prices(date: str):
    """발굴 주요 티커(Top3 + screened)의 가격을 price_cache에서 개별 갱신.

    스케줄러 분석 완료 후 장중에만 호출됩니다.
    price_cache.ensure_fresh() 가 내부적으로 KIS 우선 → Naver 폴백을 처리합니다.
    """
    from data.price_cache import price_cache

    tracked: set = set()

    # Smart Top3 티커
    for r in _analysis.get_smart_top3(date):
        tracked.add(r["ticker"])

    # Screened 티커 (상위 20개)
    screened_df = _analysis.get_screened(date)
    if screened_df is not None and not screened_df.empty:
        tracked.update(screened_df.head(20).index.tolist())

    if tracked:
        logger.info(f"[Scheduler] 발굴 종목 현재가 갱신: {len(tracked)}개")
        price_cache.ensure_fresh(list(tracked))


def _precompute_smart_top3(daily_df: pd.DataFrame, date: str):
    """멀티팩터 점수 기반 Top 종목 사전 계산."""
    from data.fetcher import get_stock_ohlcv_history, get_investor_trend_individual
    from analysis.scoring import calc_composite_score, is_anomaly_neglected_rebound

    logger.info("[Scheduler] Smart Top3 점수 계산 시작")

    has_inst = daily_df.get("기관합계_5일", pd.Series(dtype=float)).fillna(0)
    has_frgn = daily_df.get("외국인합계_5일", pd.Series(dtype=float)).fillna(0)
    supply_mask = (has_inst > 0) | (has_frgn > 0)
    candidates = daily_df[supply_mask].copy()

    if candidates.empty:
        _analysis.put_smart_top3([], date)
        return

    candidates["_supply_sum"] = has_inst[supply_mask] + has_frgn[supply_mask]
    pool = candidates.nlargest(30, "_supply_sum")

    end_dt = datetime.datetime.strptime(date, "%Y%m%d")
    start_dt = end_dt - datetime.timedelta(days=80)
    start_str = start_dt.strftime("%Y%m%d")

    results = []
    for row in pool.itertuples():
        ticker = row.Index
        try:
            ohlcv = get_stock_ohlcv_history(ticker, start_str, date)
            investor = get_investor_trend_individual(ticker)
        except Exception:
            continue

        if ohlcv.empty or len(ohlcv) < 5:
            continue

        score, details = calc_composite_score(ohlcv, investor)
        results.append({
            "ticker": ticker,
            "name": str(getattr(row, '종목명', ticker) or ticker),
            "price": float(getattr(row, '종가', 0) or 0),
            "change": float(getattr(row, '등락률', 0) or 0),
            "sector": str(getattr(row, '업종', '') or ''),
            "inst_5d": float(getattr(row, '기관합계_5일', 0) or 0),
            "frgn_5d": float(getattr(row, '외국인합계_5일', 0) or 0),
            "ohlcv": ohlcv,
            "investor": investor,
            "score": score,
            "details": details,
        })
        time.sleep(0.1)

    results.sort(key=lambda x: x["score"], reverse=True)
    _analysis.put_smart_top3(results, date)
    logger.info(f"[Scheduler] Smart Top3 완료: {len(results)}종목 점수 산출")


def _precompute_screened(daily_df: pd.DataFrame, date: str):
    """수급 스크리닝 + 기술적 지표 사전 계산."""
    from analysis.screening import screen_by_supply, add_chart_status

    logger.info("[Scheduler] 기술적 지표 스크리닝 시작")

    supply_filtered = screen_by_supply(daily_df)

    # Smart Top3 티커가 발굴 목록에 빠졌으면 강제 포함
    top3 = _analysis.get_smart_top3()
    top3_tickers = [r["ticker"] for r in top3]
    missing = [
        t for t in top3_tickers
        if t in daily_df.index and (supply_filtered.empty or t not in supply_filtered.index)
    ]
    if missing:
        extra = daily_df.loc[missing].copy()
        if "수급합계_5일" not in extra.columns:
            extra["수급합계_5일"] = (
                extra.get("기관합계_5일", 0).fillna(0)
                + extra.get("외국인합계_5일", 0).fillna(0)
            )
        supply_filtered = pd.concat([extra, supply_filtered]) if not supply_filtered.empty else extra

    if supply_filtered.empty:
        _analysis.put_screened(pd.DataFrame(), date)
        return

    # 상위 100종목까지 차트 분석 (대부분의 top_n 설정 커버)
    top_n_stocks = supply_filtered.head(100)
    screened = add_chart_status(top_n_stocks, date)
    _analysis.put_screened(screened, date)
    logger.info(f"[Scheduler] 스크리닝 완료: {len(screened)}종목 기술지표 산출")


def _precompute_program_trading():
    """프로그램 매매 데이터 사전 로드."""
    from data.fetcher import get_program_trading_top

    logger.info("[Scheduler] 프로그램 매매 데이터 로드")
    prog_df = get_program_trading_top()
    if not prog_df.empty:
        _analysis.put_program_trading(prog_df)
        logger.info(f"[Scheduler] 프로그램 매매 완료: {len(prog_df)}건")


def _precompute_theme_list():
    """테마 목록 사전 로드."""
    from data.fetcher import get_theme_list

    logger.info("[Scheduler] 테마 목록 로드")
    theme_df = get_theme_list()
    if not theme_df.empty:
        _analysis.put_theme_list(theme_df)
        logger.info(f"[Scheduler] 테마 목록 완료: {len(theme_df)}건")


def _scheduler_loop(date: str, market: str, supply_days: int):
    """백그라운드 스케줄러 루프."""
    logger.info(f"[Scheduler] 스케줄러 시작 (정시 갱신: {REFRESH_HOURS}시)")


    # 최초 즉시 갱신: 데이터 → 분석 (9시 10분에만 최초 갱신)
    _now_start = datetime.datetime.now()
    _today_str = datetime.date.today().strftime("%Y%m%d")
    _is_post_market_start = (
        date == _today_str
        and not _is_market_hours()
        and _now_start.weekday() < 5
        and _now_start.hour >= MARKET_CLOSE_HOUR
        and _is_snapshot_stale(date, market)
    )
    if _is_post_market_start:
        logger.info("[Scheduler] 장 마감 후 기동 — 스냅샷이 장중 데이터, 종가 확정 데이터로 초기 갱신")
        _do_post_market_snapshot(date, market, supply_days)
        _snapshot_saved_today = True
    elif _now_start.hour == 9 and _now_start.minute < 10:
        # 9시 10분 이전에는 최초 갱신을 하지 않음
        _snapshot_saved_today = False
    else:
        _do_refresh(date, market, supply_days)
        _snapshot_saved_today = False
    _do_analysis(date)

    last_refresh_hour = _now_start.hour  # 이미 갱신한 시간 추적


    while not _scheduler_stop.is_set():
        # 1분마다 체크
        if _scheduler_stop.wait(timeout=REFRESH_INTERVAL_SEC):
            break  # stop 요청 시 종료

        now = datetime.datetime.now()

        # 9시는 10분에, 나머지는 0분에 갱신
        refresh_minute = REFRESH_MINUTES.get(now.hour, 0)

        # 지정된 갱신 시간(REFRESH_HOURS)이며, 해당 분에, 같은 시간에 아직 갱신하지 않았으면
        if (now.hour in REFRESH_HOURS
            and now.minute == refresh_minute
            and now.hour != last_refresh_hour
            and now.weekday() < 5):  # 평일만
            logger.info(f"[Scheduler] 정시 갱신: {now.strftime('%H시 %M분')}")
            _do_refresh(date, market, supply_days)
            _do_analysis(date)
            last_refresh_hour = now.hour
            _snapshot_saved_today = False  # 장중이면 리셋 (다음 마감용)

        # ── 장 마감 직후 (16:00~17:00) 자동 스냅샷 저장 ──
        elif (now.weekday() < 5
              and MARKET_CLOSE_HOUR <= now.hour < MARKET_CLOSE_HOUR + 1
              and not _snapshot_saved_today):
            logger.info("[Scheduler] 장 마감 감지 — 전종목 스냅샷 자동 저장 시작")
            _do_post_market_snapshot(date, market, supply_days)
            # 종가 확정 데이터로 분석 재실행
            _do_analysis(date)
            _snapshot_saved_today = True

    logger.info("[Scheduler] 스케줄러 종료")


def _is_snapshot_stale(date: str, market: str) -> bool:
    """오늘 당일 스냅샷이 장 마감(16:00) 이전 저장이면 True (재fetch 필요).

    과거 날짜는 항상 False — 과거 확정 데이터는 그대로 사용.
    """
    import os
    from data.fetcher import _SNAPSHOT_DIR

    today_str = datetime.date.today().strftime("%Y%m%d")
    if date != today_str:
        return False  # 과거 날짜는 재fetch 불필요

    filepath = os.path.join(_SNAPSHOT_DIR, f"{date}_{market}.csv")
    if not os.path.exists(filepath):
        return True  # 스냅샷 없음 → API fetch 필요

    mtime_dt = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))
    return mtime_dt.hour < MARKET_CLOSE_HOUR  # 16시 이전 저장이면 stale


def _do_post_market_snapshot(date: str, market: str, supply_days: int):
    """장 마감 직후 정확한 데이터로 전종목 스냅샷 저장.

    장중 마지막 갱신 데이터가 _store에 있으면 그것을 저장하고,
    없으면 API에서 한 번 더 fetch하여 저장한다.
    """
    from data.fetcher import (
        smart_load_daily_data, _save_full_snapshot,
        load_daily_snapshot, _SNAPSHOT_DIR, _cache,
    )
    import os

    try:
        _store.is_refreshing = True

        # 캐시 전체 클리어: 종가 확정 데이터로 재fetch 보장
        for k in [k for k in list(_cache)
                  if k.startswith("stocks_") or k.startswith("integration_")]:
            _cache.pop(k, None)

        # 장마감 후 API에서 종가 확정 데이터 직접 fetch (스냅샷 유무 무관)
        daily_df = smart_load_daily_data(date, market, supply_days, force_refresh=True)
        if not daily_df.empty:
            _store.put(daily_df, date, market)
            logger.info(f"[Scheduler] 장마감 스냅샷 저장 완료: {len(daily_df)}종목")
        else:
            logger.warning("[Scheduler] 장마감 스냅샷 저장 실패: 데이터 비어있음")

    except Exception as e:
        logger.error(f"[Scheduler] 장마감 스냅샷 저장 실패: {e}")
    finally:
        _store.is_refreshing = False


def start_scheduler(date: str, market: str = "ALL", supply_days: int = 5):
    """백그라운드 데이터 갱신 스케줄러 시작.

    이미 실행 중이면 중복 시작하지 않음.
    날짜/시장이 변경되면 기존 스케줄러를 중지하고 재시작.
    """
    global _scheduler_thread

    # 이미 같은 설정으로 실행 중이면 스킵
    if _scheduler_thread is not None and _scheduler_thread.is_alive():
        if _store.has_data(date, market):
            return
        # 설정이 다르면 기존 스케줄러 중지
        stop_scheduler()

    _scheduler_stop.clear()
    _scheduler_thread = threading.Thread(
        target=_scheduler_loop,
        args=(date, market, supply_days),
        daemon=True,  # 메인 프로세스 종료 시 자동 종료
        name="DataRefreshScheduler",
    )
    _scheduler_thread.start()


def stop_scheduler():
    """스케줄러 중지."""
    global _scheduler_thread
    _scheduler_stop.set()
    if _scheduler_thread is not None:
        _scheduler_thread.join(timeout=5)
        _scheduler_thread = None


def get_cached_data() -> tuple:
    """스케줄러가 갱신한 최신 데이터 조회.

    Returns: (daily_df, date, market, updated_at)
        daily_df가 None이면 아직 데이터 없음.
    """
    return _store.get()


def is_refreshing() -> bool:
    """현재 갱신 중인지 확인."""
    return _store.is_refreshing


def get_refresh_count() -> int:
    """지금까지 갱신된 횟수."""
    return _store.refresh_count


def get_data_status() -> dict:
    """스케줄러 상태 요약."""
    df, date, market, updated_at = _store.get()
    return {
        "has_data": df is not None and not df.empty,
        "date": date,
        "market": market,
        "updated_at": updated_at,
        "is_refreshing": _store.is_refreshing,
        "refresh_count": _store.refresh_count,
        "is_market_hours": _is_market_hours(),
        "stock_count": len(df) if df is not None else 0,
        "has_analysis": _analysis.has_analysis(date or ""),
        "is_analyzing": _analysis.is_analyzing,
        "analysis_updated_at": _analysis.updated_at,
    }


# ─────────────────────────────────────────────────────────────────────
# 사전 분석 결과 조회 API
# ─────────────────────────────────────────────────────────────────────

def get_cached_smart_top3(date: str = "") -> list:
    """사전 계산된 Smart Top 3 결과 조회."""
    return _analysis.get_smart_top3(date)


def invalidate_analysis():
    """분석 캐시 초기화 — 다음 render_smart_top3 호출 시 재계산 트리거."""
    with _analysis._lock:
        _analysis._smart_top3 = []
        _analysis._screened_df = None
        _analysis._date = ""


def get_cached_screened(date: str = "") -> Optional[pd.DataFrame]:
    """사전 계산된 기술적 지표 스크리닝 결과 조회."""
    return _analysis.get_screened(date)


def get_cached_program_trading() -> Optional[pd.DataFrame]:
    """사전 로드된 프로그램 매매 데이터 조회."""
    return _analysis.get_program_trading()


def get_cached_theme_list() -> Optional[pd.DataFrame]:
    """사전 로드된 테마 목록 조회."""
    return _analysis.get_theme_list()


def is_analysis_ready(date: str = "") -> bool:
    """사전 분석이 완료되었는지 확인."""
    return _analysis.has_analysis(date)
