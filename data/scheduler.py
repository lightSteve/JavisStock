"""
백그라운드 데이터 갱신 스케줄러

- 10분 간격으로 최신 데이터를 자동 갱신
- 장중(09:00~16:00)에만 API 호출, 장 마감 후에는 스냅샷 재사용
- 스레드 안전한 데이터 저장소 제공
- API 제한/블록 방지를 위한 보수적 간격 적용
"""

import threading
import time
import logging
import datetime
from typing import Optional

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


# 글로벌 데이터 저장소 (Streamlit 프로세스 생존 주기 동안 유지)
_store = _DataStore()


# ─────────────────────────────────────────────────────────────────────
# 스케줄러
# ─────────────────────────────────────────────────────────────────────

_scheduler_thread: Optional[threading.Thread] = None
_scheduler_stop = threading.Event()

# 설정
REFRESH_INTERVAL_SEC = 600   # 10분
MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 16


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

        # 기존 인메모리 캐시 클리어 (최신 데이터를 받기 위해)
        keys_to_clear = [k for k in _cache if k.startswith("stocks_")]
        for k in keys_to_clear:
            _cache.pop(k, None)

        daily_df = smart_load_daily_data(date, market, supply_days)

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


def _scheduler_loop(date: str, market: str, supply_days: int):
    """백그라운드 스케줄러 루프."""
    logger.info(f"[Scheduler] 스케줄러 시작 (간격: {REFRESH_INTERVAL_SEC}초)")

    # 최초 즉시 갱신
    _do_refresh(date, market, supply_days)

    while not _scheduler_stop.is_set():
        # 다음 갱신까지 대기 (stop 이벤트 체크하며)
        if _scheduler_stop.wait(timeout=REFRESH_INTERVAL_SEC):
            break  # stop 요청 시 종료

        # 장중에만 API 갱신, 장 마감 후에는 스킵
        if _is_market_hours():
            _do_refresh(date, market, supply_days)
        else:
            logger.info("[Scheduler] 장 마감 시간 — API 갱신 스킵")

    logger.info("[Scheduler] 스케줄러 종료")


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
    }
