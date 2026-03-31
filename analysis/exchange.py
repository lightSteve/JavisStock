import pandas as pd
import numpy as np
import yfinance as yf
import FinanceDataReader as fdr

def fetch_usdkrw_history(period="6mo"):
    """USD/KRW 환율 최근 N개월 일봉 데이터 (yfinance, FDR fallback)"""
    try:
        df = yf.download('KRW=X', period=period, progress=False)
        if not df.empty:
            df = df.rename(columns={"Close": "환율"})
            return df
    except Exception:
        pass
    # FDR fallback
    try:
        df = fdr.DataReader('USD/KRW')
        if not df.empty:
            df = df.rename(columns={"Close": "환율"})
            if period.endswith("mo"):
                months = int(period[:-2])
                cutoff = pd.Timestamp.today() - pd.DateOffset(months=months)
                df = df[df.index >= cutoff]
            return df
    except Exception:
        pass
    return pd.DataFrame()

def calc_bollinger_macd(df, window=20):
    """볼린저 밴드(20일), MACD(12,26,9) 계산"""
    price = df["환율"]
    ma = price.rolling(window).mean()
    std = price.rolling(window).std()
    upper = ma + 2 * std
    lower = ma - 2 * std
    # MACD
    ema12 = price.ewm(span=12, adjust=False).mean()
    ema26 = price.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    hist = macd - signal
    return {
        "ma": ma,
        "upper": upper,
        "lower": lower,
        "macd": macd,
        "signal": signal,
        "hist": hist,
    }
