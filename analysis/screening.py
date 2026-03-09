"""
스크리닝 모듈 (Screening Logic)
1차 필터: 이동평균선 정배열 초기 진입
2차 필터: 기관/외국인 순매수 & 개인 순매도
3차 필터: 기술적 지표 (RSI, MACD, 볼린저밴드, 거래량급증)
"""

import pandas as pd
import numpy as np
import time

from data.fetcher import get_stock_ohlcv_history

from analysis.indicators import (
    calc_moving_averages,
    calc_all_indicators,
    check_alignment,
    check_golden_cross,
    check_volume_surge,
    check_bullish_candle,
    check_rsi_status,
    check_macd_status,
    check_bollinger_status,
)


def screen_by_supply(df: pd.DataFrame) -> pd.DataFrame:
    """
    2차 필터: 최근 5일 기관+외국인 쌍끌이 순매수 종목.
    기관합계_5일 > 0 AND 외국인합계_5일 > 0
    """
    cols_needed = ["기관합계_5일", "외국인합계_5일"]
    for c in cols_needed:
        if c not in df.columns:
            return df
    mask = (df["기관합계_5일"] > 0) & (df["외국인합계_5일"] > 0)
    result = df[mask].copy()
    result["수급합계_5일"] = result["기관합계_5일"] + result["외국인합계_5일"]
    return result.sort_values("수급합계_5일", ascending=False)


def add_chart_status(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    각 종목에 대해 기술적 지표를 전부 계산하여 추가합니다.
    컬럼 추가: 차트상태, 골든크로스, 거래량급증, RSI상태, MACD상태, 볼린저상태, RSI값
    """
    import datetime
    end_dt = datetime.datetime.strptime(date, "%Y%m%d")
    start_dt = end_dt - datetime.timedelta(days=200)
    start_str = start_dt.strftime("%Y%m%d")

    chart_status_list = []
    golden_cross_list = []
    volume_surge_list = []
    rsi_status_list = []
    macd_status_list = []
    bb_status_list = []
    rsi_value_list = []

    for ticker in df.index:
        try:
            hist = get_stock_ohlcv_history(ticker, start_str, date)
            if hist.empty or len(hist) < 20:
                chart_status_list.append("데이터부족")
                golden_cross_list.append(False)
                volume_surge_list.append(False)
                rsi_status_list.append("N/A")
                macd_status_list.append("N/A")
                bb_status_list.append("N/A")
                rsi_value_list.append(None)
                continue

            hist = calc_all_indicators(hist)
            status = check_alignment(hist)
            gc = check_golden_cross(hist)
            vs = check_volume_surge(hist)
            rsi_st = check_rsi_status(hist)
            macd_st = check_macd_status(hist)
            bb_st = check_bollinger_status(hist)
            rsi_val = hist["RSI"].iloc[-1] if "RSI" in hist.columns else None

            chart_status_list.append(status)
            golden_cross_list.append(gc)
            volume_surge_list.append(vs)
            rsi_status_list.append(rsi_st)
            macd_status_list.append(macd_st)
            bb_status_list.append(bb_st)
            rsi_value_list.append(round(rsi_val, 1) if rsi_val is not None else None)
        except Exception:
            chart_status_list.append("에러")
            golden_cross_list.append(False)
            volume_surge_list.append(False)
            rsi_status_list.append("N/A")
            macd_status_list.append("N/A")
            bb_status_list.append("N/A")
            rsi_value_list.append(None)
        time.sleep(0.15)

    df["차트상태"] = chart_status_list
    df["골든크로스"] = golden_cross_list
    df["거래량급증"] = volume_surge_list
    df["RSI상태"] = rsi_status_list
    df["MACD상태"] = macd_status_list
    df["볼린저상태"] = bb_status_list
    df["RSI값"] = rsi_value_list
    return df


def apply_technical_filters(
    df: pd.DataFrame,
    chart_filter: list = None,
    rsi_filter: list = None,
    macd_filter: list = None,
    bb_filter: list = None,
    volume_surge_only: bool = False,
) -> pd.DataFrame:
    """사이드바 필터 옵션에 따라 기술적 지표 기반 필터링."""
    result = df.copy()

    # 차트 상태 필터
    if chart_filter:
        mask = result["차트상태"].isin(chart_filter)
        if "골든크로스" in chart_filter and "골든크로스" in result.columns:
            mask = mask | result["골든크로스"]
        result = result[mask]

    # RSI 필터
    if rsi_filter and "RSI상태" in result.columns:
        result = result[result["RSI상태"].isin(rsi_filter)]

    # MACD 필터
    if macd_filter and "MACD상태" in result.columns:
        result = result[result["MACD상태"].isin(macd_filter)]

    # 볼린저 필터
    if bb_filter and "볼린저상태" in result.columns:
        result = result[result["볼린저상태"].isin(bb_filter)]

    # 거래량 급증
    if volume_surge_only and "거래량급증" in result.columns:
        result = result[result["거래량급증"]]

    return result


def run_full_screening(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    전체 스크리닝 파이프라인 실행.
    1) 수급 필터 → 2) 차트 + 기술적 상태 추가 → 3) 정배열 + 수급 양호 종목 반환
    """
    # 1차: 수급 필터 (기관+외국인 쌍끌이)
    screened = screen_by_supply(df)

    if screened.empty:
        return screened

    # 상위 100개만 차트 분석 (API 부하 방지)
    top_n = screened.head(100).copy()

    # 2차: 차트 + 기술적 지표 추가
    top_n = add_chart_status(top_n, date)

    # 최종 필터: 정배열(초기 이상) 종목
    final = top_n[top_n["차트상태"].isin(["완전정배열", "정배열초기"])].copy()
    return final
