"""
7가지 매매 전략별 종목 추천 로직

1. 스캘/돌파매매: 종가 강마감 + 거래량 급증 + 정배열
2. 종가베팅: 종가=고가 + 양봉 + 높은 거래대금
3. ABCD매매: 20일 고점 대비 조정 후 MACD 반전
4. 눌림목매매: 정배열 + MA5 눌림 + 적정 RSI
5. 상한가따라잡기: 상한가 종목의 동일 업종 후발주
6. 기관/외인 수급매매: 쌍끌이 매수 + 거래량 + 골든크로스
7. 스윙매매: 볼린저 하단 반등 + RSI 과매도 탈출
"""

import time
import datetime
from typing import List, Dict, Any, Optional

import pandas as pd
import numpy as np

from data.fetcher import get_stock_ohlcv_history
from analysis.indicators import (
    calc_all_indicators,
    check_alignment,
    check_golden_cross,
    check_volume_surge,
    check_bullish_candle,
    check_rsi_status,
    check_macd_status,
    check_bollinger_status,
)


# ═══════════════════════════════════════════════════════════════════════════
# 공통 유틸
# ═══════════════════════════════════════════════════════════════════════════

def _safe_float(val, default=0.0) -> float:
    try:
        v = float(val)
        return default if np.isnan(v) else v
    except (ValueError, TypeError):
        return default


def _fetch_ohlcv(ticker: str, date: str, days: int = 120) -> Optional[pd.DataFrame]:
    """OHLCV 히스토리를 가져옴. 실패 시 None."""
    try:
        end_dt = datetime.datetime.strptime(date, "%Y%m%d")
        start_dt = end_dt - datetime.timedelta(days=days)
        df = get_stock_ohlcv_history(ticker, start_dt.strftime("%Y%m%d"), date)
        if df.empty or len(df) < 20:
            return None
        return df
    except Exception:
        return None


def _make_result(ticker: str, row: pd.Series, score: float,
                 strategy: str, reason: str, extra: dict = None) -> dict:
    """표준 결과 딕셔너리 생성."""
    sector = row.get("업종", "") or ""
    if not isinstance(sector, str):
        sector = ""
    return {
        "ticker": ticker,
        "name": str(row.get("종목명", ticker)),
        "price": _safe_float(row.get("종가", 0)),
        "change": _safe_float(row.get("등락률", 0)),
        "volume": _safe_float(row.get("거래대금", 0)),
        "sector": sector,
        "score": round(score, 1),
        "strategy": strategy,
        "reason": reason,
        **(extra or {}),
    }


# ═══════════════════════════════════════════════════════════════════════════
# 1) 스캘/돌파매매
# ═══════════════════════════════════════════════════════════════════════════

def screen_scalp_breakout(daily_df: pd.DataFrame, top_n: int = 5) -> List[dict]:
    """종가가 당일 고가의 95% 이상 + 양봉 + 거래대금 상위."""
    df = daily_df.copy()
    required = ["종가", "고가", "시가", "등락률", "거래대금"]
    if not all(c in df.columns for c in required):
        return []

    df["_close_high_ratio"] = df["종가"] / df["고가"].replace(0, np.nan)
    df["_is_bullish"] = df["종가"] > df["시가"]

    # 필터: 종가 >= 고가×0.95, 양봉, 등락률 > 2%, 거래대금 > 0
    mask = (
        (df["_close_high_ratio"] >= 0.95) &
        (df["_is_bullish"]) &
        (df["등락률"] > 2.0) &
        (df["거래대금"] > 0)
    )
    candidates = df[mask].copy()
    if candidates.empty:
        return []

    # 점수: (종가/고가 비율 × 40) + (등락률 정규화 × 30) + (거래대금 순위 × 30)
    candidates["_tv_rank"] = candidates["거래대금"].rank(pct=True)
    candidates["_chg_norm"] = candidates["등락률"].clip(0, 20) / 20
    candidates["_score"] = (
        candidates["_close_high_ratio"] * 40 +
        candidates["_chg_norm"] * 30 +
        candidates["_tv_rank"] * 30
    )
    top = candidates.nlargest(top_n, "_score")

    results = []
    for ticker, row in top.iterrows():
        ratio = row["_close_high_ratio"]
        reason_parts = []
        reason_parts.append(f"종가/고가 {ratio:.1%}")
        reason_parts.append(f"+{row['등락률']:.1f}%")
        tv_억 = row["거래대금"] / 1e8
        reason_parts.append(f"거래대금 {tv_억:,.0f}억")
        results.append(_make_result(
            ticker, row, row["_score"],
            "스캘/돌파", " · ".join(reason_parts),
        ))
    return results


# ═══════════════════════════════════════════════════════════════════════════
# 2) 종가베팅
# ═══════════════════════════════════════════════════════════════════════════

def screen_close_betting(daily_df: pd.DataFrame, top_n: int = 5) -> List[dict]:
    """종가 = 고가 근접(1% 이내) + 양봉 + 거래대금 상위 + 적정 등락률."""
    df = daily_df.copy()
    required = ["종가", "고가", "시가", "등락률", "거래대금"]
    if not all(c in df.columns for c in required):
        return []

    df["_gap_pct"] = (df["고가"] - df["종가"]) / df["고가"].replace(0, np.nan) * 100
    df["_is_bullish"] = df["종가"] > df["시가"]

    # 종가가 고가의 1% 이내 + 양봉 + 등락률 0.5~15%
    mask = (
        (df["_gap_pct"] <= 1.0) &
        (df["_is_bullish"]) &
        (df["등락률"] >= 0.5) &
        (df["등락률"] <= 15.0) &
        (df["거래대금"] > 0)
    )
    candidates = df[mask].copy()
    if candidates.empty:
        return []

    # 거래대금 상위 20% 필터
    tv_threshold = daily_df["거래대금"].quantile(0.80)
    candidates = candidates[candidates["거래대금"] >= tv_threshold]
    if candidates.empty:
        return []

    # 점수: 고가근접도(40) + 등락률(30) + 거래대금순위(30)
    candidates["_closeness"] = (1 - candidates["_gap_pct"] / 1.0).clip(0, 1)
    candidates["_tv_rank"] = candidates["거래대금"].rank(pct=True)
    candidates["_chg_norm"] = candidates["등락률"].clip(0, 15) / 15
    candidates["_score"] = (
        candidates["_closeness"] * 40 +
        candidates["_chg_norm"] * 30 +
        candidates["_tv_rank"] * 30
    )
    top = candidates.nlargest(top_n, "_score")

    results = []
    for ticker, row in top.iterrows():
        gap = row["_gap_pct"]
        reason_parts = [
            f"고가대비 {gap:.2f}% 근접" if gap > 0 else "종가=고가",
            f"+{row['등락률']:.1f}%",
            f"거래대금 {row['거래대금']/1e8:,.0f}억",
        ]
        results.append(_make_result(
            ticker, row, row["_score"],
            "종가베팅", " · ".join(reason_parts),
        ))
    return results


# ═══════════════════════════════════════════════════════════════════════════
# 3) ABCD매매 (조정 후 반등)
# ═══════════════════════════════════════════════════════════════════════════

def screen_abcd_pattern(daily_df: pd.DataFrame, date_str: str,
                        top_n: int = 5) -> List[dict]:
    """20일 고점 대비 -5~-15% 조정 + 금일 양봉 + MACD 매수전환."""
    df = daily_df.copy()
    required = ["종가", "시가", "등락률", "거래대금"]
    if not all(c in df.columns for c in required):
        return []

    # 1차 필터: 오늘 양봉 + 등락률 양수 + 거래대금 있음
    mask = (
        (df["종가"] > df["시가"]) &
        (df["등락률"] > 0) &
        (df["거래대금"] > 0)
    )
    candidates = df[mask].copy()
    if candidates.empty:
        return []

    # 거래대금 상위 절반만
    tv_median = daily_df["거래대금"].median()
    candidates = candidates[candidates["거래대금"] >= tv_median]
    pool = candidates.nlargest(40, "거래대금")

    results = []
    for ticker, row in pool.iterrows():
        ohlcv = _fetch_ohlcv(ticker, date_str, 60)
        if ohlcv is None:
            continue

        # 20일 고점 대비 조정폭 계산
        if len(ohlcv) < 20:
            continue
        high_20d = ohlcv["고가"].iloc[-21:-1].max()
        if high_20d <= 0:
            continue
        current = ohlcv["종가"].iloc[-1]
        pullback_pct = (current - high_20d) / high_20d * 100

        # -5% ~ -15% 조정 범위
        if not (-15 <= pullback_pct <= -3):
            continue

        ohlcv = calc_all_indicators(ohlcv)
        macd_st = check_macd_status(ohlcv)

        # MACD 매수신호 또는 하락둔화
        if macd_st not in ("매수신호", "하락둔화"):
            continue

        score = 50 + abs(pullback_pct) * 2 + (20 if macd_st == "매수신호" else 10)
        score = min(score, 100)

        reason_parts = [
            f"고점대비 {pullback_pct:.1f}%",
            f"MACD {macd_st}",
            f"+{row['등락률']:.1f}% 반등",
        ]
        results.append(_make_result(
            ticker, row, score, "ABCD패턴", " · ".join(reason_parts),
        ))
        time.sleep(0.08)

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_n]


# ═══════════════════════════════════════════════════════════════════════════
# 4) 눌림목매매
# ═══════════════════════════════════════════════════════════════════════════

def screen_pullback_buy(daily_df: pd.DataFrame, date_str: str,
                        top_n: int = 5) -> List[dict]:
    """정배열 + MA5 눌림(±1.5%) + RSI 40~60 + 기관/외인 매수."""
    df = daily_df.copy()

    # 1차 필터: 수급 양호 + 소폭 등락
    inst = df.get("기관합계_5일", pd.Series(dtype=float)).fillna(0)
    frgn = df.get("외국인합계_5일", pd.Series(dtype=float)).fillna(0)
    mask = (
        ((inst > 0) | (frgn > 0)) &
        (df["등락률"].between(-2, 3)) &
        (df["거래대금"] > 0)
    )
    candidates = df[mask].copy()
    if candidates.empty:
        return []

    pool = candidates.nlargest(40, "거래대금")

    results = []
    for ticker, row in pool.iterrows():
        ohlcv = _fetch_ohlcv(ticker, date_str, 150)
        if ohlcv is None:
            continue

        ohlcv = calc_all_indicators(ohlcv)
        alignment = check_alignment(ohlcv)
        if alignment not in ("완전정배열", "정배열초기"):
            continue

        # MA5 눌림 체크: 종가가 MA5 ±1.5% 이내
        last = ohlcv.iloc[-1]
        ma5 = last.get("MA5", 0)
        close = last.get("종가", 0)
        if ma5 <= 0:
            continue
        ma5_gap = abs(close - ma5) / ma5 * 100
        if ma5_gap > 1.5:
            continue

        # RSI 40~60 (과열 아닌 적정 구간)
        rsi = last.get("RSI", 50)
        if not (35 <= rsi <= 62):
            continue

        score = 60
        score += 10 if alignment == "완전정배열" else 5
        score += max(0, (1.5 - ma5_gap) * 10)  # 근접할수록 +
        score += 5 if (inst.get(ticker, 0) > 0 and frgn.get(ticker, 0) > 0) else 0
        score = min(score, 100)

        supply = "기관+외인" if inst.get(ticker, 0) > 0 and frgn.get(ticker, 0) > 0 else \
                 "기관" if inst.get(ticker, 0) > 0 else "외인"
        reason_parts = [
            alignment,
            f"MA5 {ma5_gap:.1f}% 근접",
            f"RSI {rsi:.0f}",
            f"{supply} 매수",
        ]
        results.append(_make_result(
            ticker, row, score, "눌림목", " · ".join(reason_parts),
        ))
        time.sleep(0.08)

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_n]


# ═══════════════════════════════════════════════════════════════════════════
# 5) 상한가따라잡기
# ═══════════════════════════════════════════════════════════════════════════

def screen_limit_up_follow(daily_df: pd.DataFrame, top_n: int = 5) -> List[dict]:
    """상한가 종목의 동일 업종 내 후발주 (+5~20%)."""
    df = daily_df.copy()
    if "등락률" not in df.columns or "업종" not in df.columns:
        return []

    # 상한가 종목 찾기 (29%+)
    limit_up = df[df["등락률"] >= 29.0]
    if limit_up.empty:
        return []

    # 상한가 종목의 업종들
    limit_sectors = set()
    for _, row in limit_up.iterrows():
        sector = row.get("업종", "")
        if isinstance(sector, str) and sector:
            limit_sectors.add(sector)

    if not limit_sectors:
        return []

    # 해당 업종의 +5~20% 후발주
    followers = df[
        (df["업종"].isin(limit_sectors)) &
        (df["등락률"] >= 5.0) &
        (df["등락률"] < 29.0) &
        (df["거래대금"] > 0)
    ].copy()

    if followers.empty:
        return []

    # 점수: 등락률(40) + 거래대금(30) + 업종내 상한가 수(30)
    sector_lu_count = limit_up.groupby("업종").size()
    followers["_lu_count"] = followers["업종"].map(sector_lu_count).fillna(0)
    followers["_tv_rank"] = followers["거래대금"].rank(pct=True)
    followers["_chg_norm"] = followers["등락률"].clip(5, 20) / 20
    followers["_score"] = (
        followers["_chg_norm"] * 40 +
        followers["_tv_rank"] * 30 +
        (followers["_lu_count"] / followers["_lu_count"].max()).fillna(0) * 30
    )
    top = followers.nlargest(top_n, "_score")

    results = []
    for ticker, row in top.iterrows():
        lu_in_sector = limit_up[limit_up["업종"] == row["업종"]]
        leader_names = ", ".join(lu_in_sector["종목명"].head(2).tolist()) if "종목명" in lu_in_sector.columns else ""
        reason_parts = [
            f"업종 대장: {leader_names}" if leader_names else "업종 대장 상한가",
            f"+{row['등락률']:.1f}%",
            f"거래대금 {row['거래대금']/1e8:,.0f}억",
        ]
        results.append(_make_result(
            ticker, row, row["_score"],
            "상한가따라잡기", " · ".join(reason_parts),
        ))
    return results


# ═══════════════════════════════════════════════════════════════════════════
# 6) 기관/외인 수급매매
# ═══════════════════════════════════════════════════════════════════════════

def screen_institutional_flow(daily_df: pd.DataFrame, date_str: str,
                              top_n: int = 5) -> List[dict]:
    """기관+외인 쌍끌이 5일 순매수 + 골든크로스/정배열 + 거래량 증가."""
    df = daily_df.copy()
    inst = df.get("기관합계_5일", pd.Series(dtype=float)).fillna(0)
    frgn = df.get("외국인합계_5일", pd.Series(dtype=float)).fillna(0)

    # 기관 AND 외인 쌍끌이
    mask = (inst > 0) & (frgn > 0) & (df["거래대금"] > 0)
    candidates = df[mask].copy()
    if candidates.empty:
        return []

    candidates["_supply_sum"] = inst[mask] + frgn[mask]
    pool = candidates.nlargest(30, "_supply_sum")

    results = []
    for ticker, row in pool.iterrows():
        ohlcv = _fetch_ohlcv(ticker, date_str, 150)
        if ohlcv is None:
            continue

        ohlcv = calc_all_indicators(ohlcv)
        alignment = check_alignment(ohlcv)
        gc = check_golden_cross(ohlcv)
        vol_surge = check_volume_surge(ohlcv, threshold=1.5)

        # 최소 정배열초기 이상
        if alignment == "역배열":
            continue

        score = 50
        score += 15 if alignment == "완전정배열" else 8
        score += 15 if gc else 0
        score += 10 if vol_surge else 0
        # 수급 강도 반영
        supply_억 = row["_supply_sum"] / 1e8
        score += min(supply_억 / 10, 10)
        score = min(score, 100)

        reason_parts = [
            alignment,
        ]
        if gc:
            reason_parts.append("골든크로스")
        if vol_surge:
            reason_parts.append("거래량↑")
        inst_억 = inst.get(ticker, 0) / 1e8
        frgn_억 = frgn.get(ticker, 0) / 1e8
        reason_parts.append(f"기관 {inst_억:+,.0f}억 · 외인 {frgn_억:+,.0f}억")

        results.append(_make_result(
            ticker, row, score, "수급매매", " · ".join(reason_parts),
        ))
        time.sleep(0.08)

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_n]


# ═══════════════════════════════════════════════════════════════════════════
# 7) 스윙매매
# ═══════════════════════════════════════════════════════════════════════════

def screen_swing_trade(daily_df: pd.DataFrame, date_str: str,
                       top_n: int = 5) -> List[dict]:
    """볼린저 하단 반등 + RSI 과매도→탈출 + 60일선 위."""
    df = daily_df.copy()

    # 1차 필터: 양봉 + 소폭 반등 + 거래대금 중위 이상
    mask = (
        (df["종가"] > df["시가"]) &
        (df["등락률"].between(0.5, 10)) &
        (df["거래대금"] > 0)
    )
    candidates = df[mask].copy()
    if candidates.empty:
        return []

    tv_median = daily_df["거래대금"].median()
    candidates = candidates[candidates["거래대금"] >= tv_median]
    pool = candidates.nlargest(40, "거래대금")

    results = []
    for ticker, row in pool.iterrows():
        ohlcv = _fetch_ohlcv(ticker, date_str, 150)
        if ohlcv is None:
            continue

        ohlcv = calc_all_indicators(ohlcv)
        last = ohlcv.iloc[-1]

        # 볼린저 %B 체크: 하단 근접 (최근 5일 내 0.2 이하였다가 반등)
        if "BB_%B" not in ohlcv.columns or len(ohlcv) < 5:
            continue

        recent_bb = ohlcv["BB_%B"].tail(5)
        was_low = (recent_bb.iloc[:-1] <= 0.2).any()
        now_rising = recent_bb.iloc[-1] > 0.2
        if not (was_low and now_rising):
            continue

        # RSI: 과매도 탈출 (최근 5일 내 35 이하 → 현재 35+)
        rsi = last.get("RSI", 50)
        recent_rsi = ohlcv["RSI"].tail(5)
        was_oversold = (recent_rsi.iloc[:-1] <= 35).any()
        now_recovering = rsi > 35

        # MA60 위
        ma60 = last.get("MA60", 0)
        close = last.get("종가", 0)
        above_ma60 = close > ma60 > 0

        score = 50
        score += 15 if was_oversold and now_recovering else 5
        score += 15 if above_ma60 else 0
        bb_pctb = last.get("BB_%B", 0.5)
        score += max(0, (0.5 - bb_pctb) * 30)  # 하단에서 올라올수록 +
        score = min(score, 100)

        reason_parts = [
            f"BB 반등 (%B {bb_pctb:.2f})",
        ]
        if was_oversold and now_recovering:
            reason_parts.append(f"RSI 과매도탈출 ({rsi:.0f})")
        else:
            reason_parts.append(f"RSI {rsi:.0f}")
        if above_ma60:
            reason_parts.append("60일선 위")
        reason_parts.append(f"+{row['등락률']:.1f}%")

        results.append(_make_result(
            ticker, row, score, "스윙", " · ".join(reason_parts),
        ))
        time.sleep(0.08)

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_n]


# ═══════════════════════════════════════════════════════════════════════════
# 전체 실행 오케스트레이터
# ═══════════════════════════════════════════════════════════════════════════

ALL_STRATEGIES = [
    {"key": "scalp", "name": "스캘/돌파매매", "icon": "⚡", "func": "screen_scalp_breakout", "needs_ohlcv": False},
    {"key": "close_bet", "name": "종가베팅", "icon": "🎯", "func": "screen_close_betting", "needs_ohlcv": False},
    {"key": "abcd", "name": "ABCD매매", "icon": "📐", "func": "screen_abcd_pattern", "needs_ohlcv": True},
    {"key": "pullback", "name": "눌림목매매", "icon": "📉", "func": "screen_pullback_buy", "needs_ohlcv": True},
    {"key": "limit_follow", "name": "상한가따라잡기", "icon": "🔒", "func": "screen_limit_up_follow", "needs_ohlcv": False},
    {"key": "inst_flow", "name": "수급매매", "icon": "🏛️", "func": "screen_institutional_flow", "needs_ohlcv": True},
    {"key": "swing", "name": "스윙매매", "icon": "🌊", "func": "screen_swing_trade", "needs_ohlcv": True},
]


def run_all_strategies(daily_df: pd.DataFrame, date_str: str,
                       top_n: int = 5) -> Dict[str, List[dict]]:
    """7가지 전략을 모두 실행하여 {key: [results]} 반환."""
    funcs = {
        "screen_scalp_breakout": screen_scalp_breakout,
        "screen_close_betting": screen_close_betting,
        "screen_abcd_pattern": screen_abcd_pattern,
        "screen_pullback_buy": screen_pullback_buy,
        "screen_limit_up_follow": screen_limit_up_follow,
        "screen_institutional_flow": screen_institutional_flow,
        "screen_swing_trade": screen_swing_trade,
    }
    results = {}
    for strat in ALL_STRATEGIES:
        fn = funcs[strat["func"]]
        if strat["needs_ohlcv"]:
            results[strat["key"]] = fn(daily_df, date_str, top_n)
        else:
            results[strat["key"]] = fn(daily_df, top_n)
    return results
