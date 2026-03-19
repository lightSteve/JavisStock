"""실시간 현재가 캐시 (RealtimePriceCache)
══════════════════════════════════════════════════════════════════════════════
전 컴포넌트가 공유하는 단일 현재가 소스입니다.
중복 API 호출을 방지하고 가격 일관성을 보장합니다.

■ 데이터 소스 우선순위
  1순위 KIS API  — 장중 실시간 TR (FHKST01010100, 30초 캐시)
                   ※ KIS App Key/Secret 설정 시에만 사용.
                   ※ `is_kis_configured()` 확인 필수.

  2순위 Naver mobile API — 실시간에 근접 (basic 엔드포인트, TTL 이내 재사용)
                   URL: https://m.stock.naver.com/api/stock/{ticker}/basic

  3순위 Naver 전종목 API — 10분 간격 스케줄러 자동 갱신
                   URL: https://m.stock.naver.com/api/stocks/marketValue/{market}
                   ※ 스케줄러가 smart_load_daily_data() 성공 시 자동으로 bulk 캐시 갱신.

■ TTL 정책
  - 장중  (09:00~16:00 평일): 5분 (MARKET_TTL_SEC)
  - 장외 / 주말             : 1시간 (OFFMARKET_TTL_SEC)

■ 사용법
    from data.price_cache import price_cache

    # 스케줄러 / 데이터 로더에서 — bulk 갱신
    price_cache.update_from_dataframe(daily_df)          # Naver 전종목 데이터 인입

    # 렌더링 직전 — 필요한 티커만 선별 갱신
    price_cache.ensure_fresh(tickers)                    # TTL 초과분만 API 호출
    price_cache.apply_to_dataframe(daily_df, tickers)   # DataFrame 현재가 일괄 업데이트

    # 단일 조회
    info = price_cache.get("005930")                     # {"price": …, "change_rate": …, …}
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import datetime
import threading
import time
from typing import Dict, List, Optional

import pandas as pd


# ──────────────────────────────────────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────────────────────────────────────

MARKET_TTL_SEC: int = 300       # 장중: 5분
OFFMARKET_TTL_SEC: int = 3600   # 장외·주말: 1시간

_MARKET_OPEN_HOUR = 9
_MARKET_CLOSE_HOUR = 16


def _is_market_hours() -> bool:
    now = datetime.datetime.now()
    if now.weekday() >= 5:
        return False
    return _MARKET_OPEN_HOUR <= now.hour < _MARKET_CLOSE_HOUR


def _current_ttl() -> int:
    return MARKET_TTL_SEC if _is_market_hours() else OFFMARKET_TTL_SEC


# ──────────────────────────────────────────────────────────────────────────────
# 내부 데이터 구조
# ──────────────────────────────────────────────────────────────────────────────

class _PriceEntry:
    """단일 종목 현재가 캐시 엔트리."""

    __slots__ = ("price", "change_rate", "name", "ts", "source")

    def __init__(self, price: int, change_rate: float, name: str, source: str):
        self.price = price
        self.change_rate = change_rate
        self.name = name
        self.ts = time.monotonic()
        # source: 'kis' | 'naver_rt' | 'naver_bulk' | 'snapshot'
        self.source = source

    def is_fresh(self, ttl: int) -> bool:
        return (time.monotonic() - self.ts) < ttl


# ──────────────────────────────────────────────────────────────────────────────
# PriceCache
# ──────────────────────────────────────────────────────────────────────────────

class RealtimePriceCache:
    """전 컴포넌트 공유 현재가 캐시 (Thread-safe Singleton).

    소스 태그 의미:
      'naver_bulk' — 스케줄러가 전종목 API로 갱신 (10분 주기)
      'naver_rt'   — Naver basic 엔드포인트로 개별 갱신
      'kis'        — KIS TR FHKST01010100으로 개별 갱신 (가장 신선)
      'snapshot'   — 로컬 CSV 스냅샷에서 로드 (장마감 후 정확한 종가)
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._entries: Dict[str, _PriceEntry] = {}
        self._bulk_ts: Optional[float] = None  # 마지막 bulk 갱신 monotonic 시각

    # ── 쓰기 ─────────────────────────────────────────────────────────────────

    def update_from_dataframe(
        self,
        df: pd.DataFrame,
        source: str = "naver_bulk",
    ) -> int:
        """daily_df(전종목 Naver/스냅샷 데이터)에서 가격을 일괄 캐시에 인입.

        스케줄러가 smart_load_daily_data() 성공 후 자동으로 호출합니다.
        반환: 캐시에 저장된 종목 수.
        """
        if df.empty or "종가" not in df.columns:
            return 0

        now_ts = time.monotonic()
        count = 0

        with self._lock:
            for ticker, row in df.iterrows():
                price = int(row.get("종가", 0) or 0)
                if price <= 0:
                    continue
                rate = float(row.get("등락률", 0.0) or 0.0)
                name = str(row.get("종목명", ticker) or ticker)

                entry = _PriceEntry(price, rate, name, source)
                entry.ts = now_ts
                self._entries[str(ticker)] = entry
                count += 1

            self._bulk_ts = now_ts

        return count

    def set_price(
        self,
        ticker: str,
        price: int,
        change_rate: float,
        name: str = "",
        source: str = "naver_rt",
    ) -> None:
        """단일 종목 가격 직접 설정 (개별 API 응답 반영용)."""
        with self._lock:
            existing = self._entries.get(str(ticker))
            resolved_name = name or (existing.name if existing else str(ticker))
            self._entries[str(ticker)] = _PriceEntry(
                price, change_rate, resolved_name, source
            )

    # ── 읽기 ─────────────────────────────────────────────────────────────────

    def get(self, ticker: str) -> Optional[dict]:
        """단일 종목 캐시 조회. TTL 만료 시 None 반환."""
        ttl = _current_ttl()
        with self._lock:
            entry = self._entries.get(str(ticker))
            if entry is None or not entry.is_fresh(ttl):
                return None
            return {
                "price": entry.price,
                "change_rate": entry.change_rate,
                "name": entry.name,
                "source": entry.source,
            }

    # ── TTL 확인 ─────────────────────────────────────────────────────────────

    def stale_tickers(self, tickers: List[str]) -> List[str]:
        """TTL 초과(또는 캐시 없음) 티커 목록 반환."""
        ttl = _current_ttl()
        stale = []
        with self._lock:
            for t in tickers:
                entry = self._entries.get(str(t))
                if entry is None or not entry.is_fresh(ttl):
                    stale.append(t)
        return stale

    def needs_refresh(self, tickers: List[str]) -> bool:
        """지정 티커 중 하나라도 TTL 초과이면 True."""
        return bool(self.stale_tickers(tickers))

    def last_bulk_updated(self) -> Optional[datetime.datetime]:
        """마지막 bulk(전종목) 갱신 시각 (wall-clock). 없으면 None."""
        with self._lock:
            if self._bulk_ts is None:
                return None
            elapsed = time.monotonic() - self._bulk_ts
            return datetime.datetime.now() - datetime.timedelta(seconds=elapsed)

    # ── 갱신 ─────────────────────────────────────────────────────────────────

    def ensure_fresh(
        self,
        tickers: List[str],
    ) -> Dict[str, dict]:
        """TTL 초과 티커만 API를 호출해 캐시를 갱신하고, 전체 최신 결과를 반환.

        소스 우선순위: KIS (장중, 설정 시) → Naver basic

        반환: {ticker: {"price": int, "change_rate": float, "name": str, "source": str}}
              TTL 이내 캐시가 있으면 API 호출 없이 캐시 값 반환.
        """
        # 지연 import — 순환참조 방지
        from data.fetcher import (
            get_realtime_price,
            get_kis_realtime_price,
            is_kis_configured,
        )

        stale = self.stale_tickers(tickers)
        kis_ok = is_kis_configured()

        for ticker in stale:
            price_info: dict = {}
            source = "naver_rt"

            # 1순위: KIS
            if kis_ok:
                kis_info = get_kis_realtime_price(ticker)
                if kis_info and kis_info.get("price", 0) > 0:
                    price_info = kis_info
                    source = "kis"

            # 2순위: Naver basic
            if not price_info or price_info.get("price", 0) == 0:
                naver_info = get_realtime_price(ticker)
                if naver_info and naver_info.get("price", 0) > 0:
                    price_info = naver_info
                    source = "naver_rt"

            if price_info and price_info.get("price", 0) > 0:
                self.set_price(
                    ticker,
                    int(price_info["price"]),
                    float(price_info.get("change_rate", 0.0)),
                    str(price_info.get("name", ticker)),
                    source,
                )

            time.sleep(0.08)  # API 레이트 리밋 방지

        # 캐시에서 전체 결과 조회 (fresh 여부 무관)
        result: Dict[str, dict] = {}
        ttl = _current_ttl()
        with self._lock:
            for t in tickers:
                entry = self._entries.get(str(t))
                if entry is not None and entry.is_fresh(ttl):
                    result[t] = {
                        "price": entry.price,
                        "change_rate": entry.change_rate,
                        "name": entry.name,
                        "source": entry.source,
                    }
        return result

    # ── DataFrame 반영 ────────────────────────────────────────────────────────

    def apply_to_dataframe(
        self,
        df: pd.DataFrame,
        tickers: Optional[List[str]] = None,
        price_col: str = "종가",
        rate_col: str = "등락률",
    ) -> int:
        """캐시 최신 가격을 DataFrame에 in-place 반영.

        tickers: None이면 df 전체 인덱스 대상.
        반환: 실제 업데이트된 행 수.
        """
        if df.empty or price_col not in df.columns:
            return 0

        ttl = _current_ttl()
        targets = tickers if tickers is not None else df.index.tolist()
        count = 0

        with self._lock:
            for ticker in targets:
                if ticker not in df.index:
                    continue
                entry = self._entries.get(str(ticker))
                if entry is None or not entry.is_fresh(ttl) or entry.price <= 0:
                    continue
                df.at[ticker, price_col] = entry.price
                if rate_col in df.columns:
                    df.at[ticker, rate_col] = entry.change_rate
                count += 1

        return count


# ──────────────────────────────────────────────────────────────────────────────
# 모듈 레벨 싱글톤 — 애플리케이션 전역에서 공유
# ──────────────────────────────────────────────────────────────────────────────

price_cache = RealtimePriceCache()
