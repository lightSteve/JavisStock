"""
기술적 지표 계산 모듈 (Technical Indicators)
- 이동평균선 (SMA)
- RSI (Relative Strength Index)
- MACD (Moving Average Convergence Divergence)
- 볼린저 밴드 (Bollinger Bands)
- 정배열 판별
- 거래량 급증 판별
"""

import pandas as pd
import numpy as np


def calc_moving_averages(df: pd.DataFrame, col: str = "종가") -> pd.DataFrame:
    """
    종가 기준으로 5, 20, 60, 120일 이동평균선을 계산하여 컬럼을 추가합니다.
    df: index=날짜, '종가' 컬럼 필수
    """
    for window in [5, 20, 60, 120]:
        df[f"MA{window}"] = df[col].rolling(window=window, min_periods=1).mean()
    return df


# ---------------------------------------------------------------------------
# RSI (Relative Strength Index)
# ---------------------------------------------------------------------------

def calc_rsi(df: pd.DataFrame, period: int = 14, col: str = "종가") -> pd.DataFrame:
    """RSI를 계산해 'RSI' 컬럼을 추가합니다."""
    delta = df[col].diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["RSI"] = 100 - (100 / (1 + rs))
    df["RSI"] = df["RSI"].fillna(50)
    return df


def check_rsi_status(df: pd.DataFrame) -> str:
    """RSI 상태 판별: 과매수(>70) / 과매도(<30) / 중립"""
    if "RSI" not in df.columns or len(df) < 2:
        return "N/A"
    rsi = df["RSI"].iloc[-1]
    if rsi >= 70:
        return "과매수"
    elif rsi <= 30:
        return "과매도"
    elif rsi >= 60:
        return "강세"
    elif rsi <= 40:
        return "약세"
    return "중립"


# ---------------------------------------------------------------------------
# MACD (Moving Average Convergence Divergence)
# ---------------------------------------------------------------------------

def calc_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26,
              signal: int = 9, col: str = "종가") -> pd.DataFrame:
    """MACD, Signal, Histogram 컬럼을 추가합니다."""
    ema_fast = df[col].ewm(span=fast, min_periods=fast).mean()
    ema_slow = df[col].ewm(span=slow, min_periods=slow).mean()
    df["MACD"] = ema_fast - ema_slow
    df["MACD_Signal"] = df["MACD"].ewm(span=signal, min_periods=signal).mean()
    df["MACD_Hist"] = df["MACD"] - df["MACD_Signal"]
    return df


def check_macd_status(df: pd.DataFrame) -> str:
    """MACD 상태 판별."""
    if "MACD" not in df.columns or len(df) < 3:
        return "N/A"
    hist = df["MACD_Hist"].iloc[-1]
    prev_hist = df["MACD_Hist"].iloc[-2]

    if hist > 0 and prev_hist <= 0:
        return "매수신호"
    elif hist < 0 and prev_hist >= 0:
        return "매도신호"
    elif hist > 0 and hist > prev_hist:
        return "상승강화"
    elif hist > 0 and hist < prev_hist:
        return "상승둔화"
    elif hist < 0 and hist < prev_hist:
        return "하락강화"
    elif hist < 0 and hist > prev_hist:
        return "하락둔화"
    return "중립"


# ---------------------------------------------------------------------------
# 볼린저 밴드 (Bollinger Bands)
# ---------------------------------------------------------------------------

def calc_bollinger_bands(df: pd.DataFrame, period: int = 20,
                         num_std: float = 2.0, col: str = "종가") -> pd.DataFrame:
    """볼린저 밴드 상단/중간/하단을 추가합니다."""
    sma = df[col].rolling(window=period, min_periods=1).mean()
    std = df[col].rolling(window=period, min_periods=1).std()
    df["BB_Upper"] = sma + num_std * std
    df["BB_Middle"] = sma
    df["BB_Lower"] = sma - num_std * std
    df["BB_%B"] = (df[col] - df["BB_Lower"]) / (df["BB_Upper"] - df["BB_Lower"]).replace(0, np.nan)
    return df


def check_bollinger_status(df: pd.DataFrame) -> str:
    """볼린저 밴드 상태 판별."""
    if "BB_%B" not in df.columns or len(df) < 2:
        return "N/A"
    pct_b = df["BB_%B"].iloc[-1]
    if pd.isna(pct_b):
        return "N/A"
    if pct_b >= 1.0:
        return "상단돌파"
    elif pct_b >= 0.8:
        return "상단근접"
    elif pct_b <= 0.0:
        return "하단돌파"
    elif pct_b <= 0.2:
        return "하단근접"
    return "중간"


# ---------------------------------------------------------------------------
# 종합 기술적 분석
# ---------------------------------------------------------------------------

def calc_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """모든 기술적 지표를 한번에 계산합니다."""
    df = calc_moving_averages(df)
    df = calc_rsi(df)
    df = calc_macd(df)
    df = calc_bollinger_bands(df)
    return df


def calc_multi_period_returns(df: pd.DataFrame, col: str = "종가") -> dict:
    """
    다기간 수익률 계산.
    반환: {"5일수익률": float, "20일수익률": float, "60일수익률": float}
    값은 퍼센트 단위 (예: 3.5 = +3.5%)
    """
    result = {"5일수익률": None, "20일수익률": None, "60일수익률": None}
    if df.empty or col not in df.columns:
        return result
    latest = df[col].iloc[-1]
    if latest == 0:
        return result
    for period, key in [(5, "5일수익률"), (20, "20일수익률"), (60, "60일수익률")]:
        if len(df) > period:
            past = df[col].iloc[-(period + 1)]
            if past > 0:
                result[key] = round((latest - past) / past * 100, 2)
    return result


def get_technical_summary(df: pd.DataFrame) -> dict:
    """모든 기술적 상태 + 다기간 수익률을 딕셔너리로 반환."""
    base = {
        "정배열": check_alignment(df),
        "골든크로스": check_golden_cross(df),
        "RSI": check_rsi_status(df),
        "MACD": check_macd_status(df),
        "볼린저": check_bollinger_status(df),
        "거래량급증": check_volume_surge(df),
        "양봉": check_bullish_candle(df),
        "RSI값": round(df["RSI"].iloc[-1], 1) if "RSI" in df.columns and len(df) > 0 else None,
    }
    base.update(calc_multi_period_returns(df))
    return base


def check_golden_cross(df: pd.DataFrame) -> bool:
    """
    최근 시점에서 20일선이 60일선을 상향 돌파(골든크로스)했는지 판별.
    최근 3일 이내에 크로스가 발생했으면 True.
    """
    if len(df) < 65:
        return False
    recent = df.tail(5)
    ma20 = recent["MA20"].values
    ma60 = recent["MA60"].values
    # 최근 5일 중 20일선이 60일선을 상향 돌파한 시점이 있는지
    for i in range(1, len(ma20)):
        if ma20[i] >= ma60[i] and ma20[i - 1] < ma60[i - 1]:
            return True
    return False


def check_alignment(df: pd.DataFrame) -> str:
    """
    최신 시점 이동평균 정배열 상태를 판별합니다.

    반환값:
    - '완전정배열': 종가 > MA5 > MA20 > MA60 > MA120
    - '정배열초기': 종가 > MA20 > MA60
    - '역배열': 그 외
    """
    if len(df) < 2:
        return "데이터부족"

    last = df.iloc[-1]
    price = last.get("종가", 0)
    ma5 = last.get("MA5", 0)
    ma20 = last.get("MA20", 0)
    ma60 = last.get("MA60", 0)
    ma120 = last.get("MA120", 0)

    if price > ma5 > ma20 > ma60 > ma120 and ma120 > 0:
        return "완전정배열"
    elif price > ma20 > ma60 and ma60 > 0:
        return "정배열초기"
    else:
        return "역배열"


def check_volume_surge(df: pd.DataFrame, threshold: float = 2.0) -> bool:
    """
    최근 거래일의 거래량이 직전 20일 평균 대비 threshold(배수) 이상인지 확인.
    """
    if len(df) < 22:
        return False
    avg_vol = df["거래량"].iloc[-21:-1].mean()
    if avg_vol == 0:
        return False
    latest_vol = df["거래량"].iloc[-1]
    return (latest_vol / avg_vol) >= threshold


def check_bullish_candle(df: pd.DataFrame) -> bool:
    """최근 봉이 양봉인지 확인."""
    if df.empty:
        return False
    last = df.iloc[-1]
    return last.get("종가", 0) > last.get("시가", 0)
