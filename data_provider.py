"""
데이터 접근 추상화 계층 (Data Provider)

모든 기능에서 직접 API를 호출하지 않고, DataProvider 인터페이스만 사용하도록 설계.
나중에 한국 증권사 API(키움 OpenAPI 등)로 교체할 수 있도록 추상화.

구현체:
  - NaverDataProvider : 기존 data.fetcher 래핑 (현재 기본)
  - CsvDataProvider   : CSV/Parquet 파일 기반 (오프라인/백테스트)
  - YFinanceDataProvider : yfinance 기반 글로벌 주식 (구조 확인용)
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
import pandas as pd


# ═══════════════════════════════════════════════════════════════════════════
# 추상 베이스 클래스
# ═══════════════════════════════════════════════════════════════════════════

class DataProviderBase(ABC):
    """데이터 접근 추상화 인터페이스.

    모든 메서드는 pandas DataFrame 또는 dict를 반환하며,
    내부 구현(API, CSV, DB 등)에 독립적으로 호출할 수 있다.
    """

    # ── 시세 ──────────────────────────────────────────────────────

    @abstractmethod
    def get_market_ohlcv(self, date: str, market: str = "ALL") -> pd.DataFrame:
        """전종목 시세 (index=티커, cols=[시가,고가,저가,종가,거래량,거래대금,등락률,시가총액])."""
        ...

    @abstractmethod
    def get_stock_ohlcv_history(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        """개별 종목 일봉 히스토리 (index=날짜, cols=[시가,고가,저가,종가,거래량,등락률])."""
        ...

    # ── 종목 메타 ─────────────────────────────────────────────────

    @abstractmethod
    def get_all_tickers(self, date: str, market: str = "ALL") -> pd.DataFrame:
        """전종목 티커·종목명 (cols=[티커,종목명,시장,종목유형])."""
        ...

    @abstractmethod
    def get_stock_name(self, ticker: str) -> str:
        """티커 → 종목명."""
        ...

    # ── 수급 ──────────────────────────────────────────────────────

    @abstractmethod
    def get_accumulated_investor_trading(
        self, end_date: str, days: int = 5, market: str = "ALL"
    ) -> pd.DataFrame:
        """누적 기관/외국인/개인 순매수 (index=티커, cols=[기관합계,외국인합계,개인])."""
        ...

    @abstractmethod
    def get_investor_trend_individual(self, ticker: str) -> pd.DataFrame:
        """개별 종목 투자자 동향 (index=날짜, cols=[기관합계,외국인합계,개인])."""
        ...

    # ── 업종/테마 ─────────────────────────────────────────────────

    @abstractmethod
    def get_sector_info(self, date: str, market: str = "ALL") -> pd.DataFrame:
        """전종목 업종 매핑 (index=티커, cols=[업종])."""
        ...

    @abstractmethod
    def get_theme_list(self) -> pd.DataFrame:
        """테마 리스트 (cols=[테마명,테마번호,등락률])."""
        ...

    @abstractmethod
    def get_theme_constituents(self, theme_no: str) -> pd.DataFrame:
        """테마 구성 종목 (cols=[티커,종목명,현재가,등락률,거래대금])."""
        ...

    # ── 뉴스 ──────────────────────────────────────────────────────

    @abstractmethod
    def get_stock_news(self, ticker: str, count: int = 10) -> List[Dict]:
        """종목별 뉴스 리스트 [{title, date, source, url}]."""
        ...

    # ── 펀더멘털 ──────────────────────────────────────────────────

    @abstractmethod
    def get_stock_fundamentals(self, ticker: str) -> Dict:
        """PER, EPS, PBR, 배당수익률, 분기실적."""
        ...

    # ── 프로그램/기타 ─────────────────────────────────────────────

    @abstractmethod
    def get_program_trading(self) -> pd.DataFrame:
        """프로그램 매매 상위 (cols=[티커,종목명,프로그램순매수,유형])."""
        ...

    # ── 통합 데이터 빌더 ──────────────────────────────────────────

    @abstractmethod
    def build_daily_dataset(self, date: str, market: str = "ALL", supply_days: int = 5) -> pd.DataFrame:
        """전종목 시세 + 수급 + 업종 결합 데이터셋."""
        ...


# ═══════════════════════════════════════════════════════════════════════════
# NaverDataProvider — 기존 fetcher 래핑
# ═══════════════════════════════════════════════════════════════════════════

class NaverDataProvider(DataProviderBase):
    """Naver Finance API 기반 DataProvider (기존 data.fetcher 래핑)."""

    def __init__(self):
        from data import fetcher as _f
        self._f = _f

    def get_market_ohlcv(self, date: str, market: str = "ALL") -> pd.DataFrame:
        return self._f.get_market_ohlcv(date, market)

    def get_stock_ohlcv_history(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        return self._f.get_stock_ohlcv_history(ticker, start, end)

    def get_all_tickers(self, date: str, market: str = "ALL") -> pd.DataFrame:
        return self._f.get_all_tickers(date, market)

    def get_stock_name(self, ticker: str) -> str:
        return self._f.get_stock_name(ticker)

    def get_accumulated_investor_trading(
        self, end_date: str, days: int = 5, market: str = "ALL"
    ) -> pd.DataFrame:
        return self._f.get_accumulated_investor_trading(end_date, days, market)

    def get_investor_trend_individual(self, ticker: str) -> pd.DataFrame:
        return self._f.get_investor_trend_individual(ticker)

    def get_sector_info(self, date: str, market: str = "ALL") -> pd.DataFrame:
        return self._f.get_sector_info(date, market)

    def get_theme_list(self) -> pd.DataFrame:
        return self._f.get_theme_list()

    def get_theme_constituents(self, theme_no: str) -> pd.DataFrame:
        return self._f.get_theme_constituents(theme_no)

    def get_stock_news(self, ticker: str, count: int = 10) -> List[Dict]:
        return self._f.get_stock_news_list(ticker, count)

    def get_stock_fundamentals(self, ticker: str) -> Dict:
        return self._f.get_stock_fundamentals(ticker)

    def get_program_trading(self) -> pd.DataFrame:
        return self._f.get_program_trading_top()

    def build_daily_dataset(self, date: str, market: str = "ALL", supply_days: int = 5) -> pd.DataFrame:
        return self._f.smart_load_daily_data(date, market, supply_days)


# ═══════════════════════════════════════════════════════════════════════════
# CsvDataProvider — CSV/Parquet 기반 (오프라인/백테스트)
# ═══════════════════════════════════════════════════════════════════════════

class CsvDataProvider(DataProviderBase):
    """CSV/Parquet 파일 기반 DataProvider.

    data_dir 아래에 다음 파일이 있으면 로드:
      - ohlcv_{YYYYMMDD}_{market}.csv   : 전종목 시세
      - history_{ticker}.csv            : 개별 종목 히스토리
      - supply_{YYYYMMDD}_{market}.csv  : 투자자 수급
      - sectors.csv                     : 업종 매핑
      - themes.csv / theme_{no}.csv     : 테마 데이터
      - news_{ticker}.csv               : 뉴스 Mock
    """

    def __init__(self, data_dir: str = "csv_data"):
        import os
        self._dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def _load_csv(self, filename: str, index_col: Optional[str] = None) -> pd.DataFrame:
        import os
        path = os.path.join(self._dir, filename)
        if not os.path.exists(path):
            return pd.DataFrame()
        return pd.read_csv(path, index_col=index_col, encoding="utf-8-sig")

    def get_market_ohlcv(self, date: str, market: str = "ALL") -> pd.DataFrame:
        return self._load_csv(f"ohlcv_{date}_{market}.csv", index_col="티커")

    def get_stock_ohlcv_history(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        df = self._load_csv(f"history_{ticker}.csv")
        if df.empty:
            return df
        df["날짜"] = pd.to_datetime(df["날짜"])
        df = df.set_index("날짜").sort_index()
        return df[start:end]

    def get_all_tickers(self, date: str, market: str = "ALL") -> pd.DataFrame:
        return self._load_csv(f"tickers_{market}.csv")

    def get_stock_name(self, ticker: str) -> str:
        return ticker

    def get_accumulated_investor_trading(
        self, end_date: str, days: int = 5, market: str = "ALL"
    ) -> pd.DataFrame:
        return self._load_csv(f"supply_{end_date}_{market}.csv", index_col="티커")

    def get_investor_trend_individual(self, ticker: str) -> pd.DataFrame:
        return self._load_csv(f"investor_{ticker}.csv")

    def get_sector_info(self, date: str, market: str = "ALL") -> pd.DataFrame:
        return self._load_csv("sectors.csv", index_col="티커")

    def get_theme_list(self) -> pd.DataFrame:
        return self._load_csv("themes.csv")

    def get_theme_constituents(self, theme_no: str) -> pd.DataFrame:
        return self._load_csv(f"theme_{theme_no}.csv")

    def get_stock_news(self, ticker: str, count: int = 10) -> List[Dict]:
        df = self._load_csv(f"news_{ticker}.csv")
        if df.empty:
            return []
        return df.head(count).to_dict("records")

    def get_stock_fundamentals(self, ticker: str) -> Dict:
        return {"PER": "", "EPS": "", "PBR": "", "배당수익률": "", "분기실적": []}

    def get_program_trading(self) -> pd.DataFrame:
        return self._load_csv("program_trading.csv")

    def build_daily_dataset(self, date: str, market: str = "ALL", supply_days: int = 5) -> pd.DataFrame:
        ohlcv = self.get_market_ohlcv(date, market)
        if ohlcv.empty:
            return ohlcv
        tickers = self.get_all_tickers(date, market)
        if not tickers.empty and "티커" in tickers.columns:
            tickers = tickers.set_index("티커")
            ohlcv = ohlcv.join(tickers[["종목명"]], how="left")
        supply = self.get_accumulated_investor_trading(date, supply_days, market)
        if not supply.empty:
            supply.columns = [f"{c}_5일" for c in supply.columns]
            ohlcv = ohlcv.join(supply, how="left")
        sectors = self.get_sector_info(date, market)
        if not sectors.empty:
            ohlcv = ohlcv.join(sectors, how="left")
        fill_cols = [c for c in ohlcv.columns if "5일" in c]
        ohlcv[fill_cols] = ohlcv[fill_cols].fillna(0)
        return ohlcv


# ═══════════════════════════════════════════════════════════════════════════
# YFinanceDataProvider — 글로벌 주식 구조 확인용 스텁
# ═══════════════════════════════════════════════════════════════════════════

class YFinanceDataProvider(DataProviderBase):
    """yfinance 기반 글로벌 주식 DataProvider (구조 확인용).

    한국 주식용은 아니지만, 인터페이스 테스트 및
    미국 주식 추가 시 사용할 수 있도록 기본 구현.
    """

    def get_market_ohlcv(self, date: str, market: str = "ALL") -> pd.DataFrame:
        return pd.DataFrame()  # 전종목 시세는 yfinance에서 미지원

    def get_stock_ohlcv_history(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        try:
            import yfinance as yf
            import datetime
            s = datetime.datetime.strptime(start, "%Y%m%d")
            e = datetime.datetime.strptime(end, "%Y%m%d")
            data = yf.download(ticker, start=s, end=e, progress=False)
            if data.empty:
                return pd.DataFrame()
            df = pd.DataFrame({
                "시가": data["Open"].values.flatten(),
                "고가": data["High"].values.flatten(),
                "저가": data["Low"].values.flatten(),
                "종가": data["Close"].values.flatten(),
                "거래량": data["Volume"].values.flatten(),
            }, index=data.index)
            df.index.name = "날짜"
            return df
        except Exception:
            return pd.DataFrame()

    def get_all_tickers(self, date: str, market: str = "ALL") -> pd.DataFrame:
        return pd.DataFrame()

    def get_stock_name(self, ticker: str) -> str:
        return ticker

    def get_accumulated_investor_trading(
        self, end_date: str, days: int = 5, market: str = "ALL"
    ) -> pd.DataFrame:
        return pd.DataFrame()

    def get_investor_trend_individual(self, ticker: str) -> pd.DataFrame:
        return pd.DataFrame()

    def get_sector_info(self, date: str, market: str = "ALL") -> pd.DataFrame:
        return pd.DataFrame()

    def get_theme_list(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_theme_constituents(self, theme_no: str) -> pd.DataFrame:
        return pd.DataFrame()

    def get_stock_news(self, ticker: str, count: int = 10) -> List[Dict]:
        return []

    def get_stock_fundamentals(self, ticker: str) -> Dict:
        return {"PER": "", "EPS": "", "PBR": "", "배당수익률": "", "분기실적": []}

    def get_program_trading(self) -> pd.DataFrame:
        return pd.DataFrame()

    def build_daily_dataset(self, date: str, market: str = "ALL", supply_days: int = 5) -> pd.DataFrame:
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════
# 팩토리 함수
# ═══════════════════════════════════════════════════════════════════════════

_default_provider: Optional[DataProviderBase] = None


def get_data_provider(provider_type: str = "naver", **kwargs) -> DataProviderBase:
    """DataProvider 인스턴스를 반환하는 팩토리 함수.

    Args:
        provider_type: "naver" | "csv" | "yfinance"
        **kwargs: 각 Provider 초기화 인자 (예: data_dir="/path/to/csv")

    Returns:
        DataProviderBase 구현체
    """
    global _default_provider
    providers = {
        "naver": NaverDataProvider,
        "csv": CsvDataProvider,
        "yfinance": YFinanceDataProvider,
    }
    cls = providers.get(provider_type)
    if cls is None:
        raise ValueError(f"지원하지 않는 provider_type: {provider_type}. 가능: {list(providers.keys())}")
    instance = cls(**kwargs)
    _default_provider = instance
    return instance


def get_default_provider() -> DataProviderBase:
    """현재 설정된 기본 DataProvider를 반환. 없으면 NaverDataProvider 생성."""
    global _default_provider
    if _default_provider is None:
        _default_provider = NaverDataProvider()
    return _default_provider
