"""
📊 TrendCatcher – 주식 모멘텀 & 수급 분석 대시보드
메인 Streamlit 앱
"""

import streamlit as st
import pandas as pd
import sys
import os

# 프로젝트 루트를 path에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.fetcher import (
    build_daily_dataset,
    get_accumulated_investor_trading,
    get_market_ohlcv,
    get_all_tickers,
    get_sector_info,
    get_latest_trading_date,
    save_daily_snapshot,
    list_available_snapshots,
)
from analysis.screening import screen_by_supply, add_chart_status, run_full_screening, apply_technical_filters
from components.sidebar import render_sidebar
from components.heatmap import render_sector_heatmap, render_sector_bar_chart
from components.top_picks import render_top_cards, render_screened_table
from components.detail import render_detail_view

from components.rising_stocks import render_rising_stocks

# ===========================================================================
# 페이지 설정
# ===========================================================================
st.set_page_config(
    page_title="TrendCatcher 📊",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 커스텀 CSS
st.markdown(
    """
    <style>
    /* 전체 배경 — 밝은 다크 테마 */
    .stApp {
        background-color: #f8f9fc;
        color: #1a1a2e;
    }
    /* 사이드바 */
    section[data-testid="stSidebar"] {
        background-color: #1e293b;
        color: #e2e8f0;
    }
    section[data-testid="stSidebar"] .stMarkdown,
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] .stSlider label,
    section[data-testid="stSidebar"] span {
        color: #e2e8f0 !important;
    }
    /* 헤더 */
    .main-header {
        background: linear-gradient(90deg, #4f46e5 0%, #7c3aed 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2.5em;
        font-weight: 800;
        text-align: center;
        padding: 10px 0;
    }
    .sub-header {
        text-align: center;
        color: #64748b;
        font-size: 1.1em;
        margin-bottom: 20px;
    }
    /* 탭 스타일 */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #f1f5f9;
        border-radius: 12px;
        padding: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 20px;
        color: #475569;
        font-weight: 600;
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background-color: #fff;
        color: #4f46e5;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    /* 메트릭 카드 */
    [data-testid="stMetric"] {
        background-color: #ffffff;
        border-radius: 12px;
        padding: 18px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    [data-testid="stMetric"] label {
        color: #64748b !important;
        font-weight: 600;
    }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #1e293b !important;
        font-weight: 700;
    }
    /* 데이터프레임 */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
    }
    /* 마크다운 텍스트 */
    .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
        color: #1e293b;
    }
    .stMarkdown p, .stMarkdown li {
        color: #334155;
    }
    /* 버튼 */
    .stButton button[kind="primary"] {
        background-color: #4f46e5;
        color: white;
        border: none;
        border-radius: 8px;
    }
    /* 정보/경고 박스 */
    .stAlert {
        border-radius: 10px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ===========================================================================
# 헤더
# ===========================================================================
st.markdown('<div class="main-header">📊 TrendCatcher</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-header">전종목 우상향 & 수급 분석 대시보드 — 가격과 돈의 흐름을 한눈에</div>',
    unsafe_allow_html=True,
)

# ===========================================================================
# 사이드바 설정값
# ===========================================================================
config = render_sidebar()
date_str = config["date"]
market = config["market"]
supply_days = config["supply_days"]
chart_filter = config["chart_filter"]
rsi_filter = config.get("rsi_filter", [])
macd_filter = config.get("macd_filter", [])
bb_filter = config.get("bb_filter", [])
volume_surge_only = config["volume_surge"]
top_n = config["top_n"]

# ===========================================================================
# 데이터 로딩 (캐시 활용)
# ===========================================================================

@st.cache_data(ttl=3600, show_spinner="📡 전종목 시세 데이터 수집 중...")
def load_ohlcv(date: str, mkt: str):
    return get_market_ohlcv(date, mkt)


@st.cache_data(ttl=3600, show_spinner="📡 종목 정보 수집 중...")
def load_tickers(date: str, mkt: str):
    return get_all_tickers(date, mkt)


@st.cache_data(ttl=3600, show_spinner="💰 수급 데이터 수집 중 (시간이 걸릴 수 있습니다)...")
def load_supply(date: str, days: int, mkt: str):
    return get_accumulated_investor_trading(date, days, mkt)


@st.cache_data(ttl=3600, show_spinner="🏭 섹터 정보 수집 중...")
def load_sectors(date: str, mkt: str):
    return get_sector_info(date, mkt)


@st.cache_data(ttl=3600, show_spinner="🔍 스크리닝 실행 중...")
def run_screening(daily_data_json: str, date: str):
    """캐시를 위해 JSON 직렬화된 데이터를 받아 스크리닝."""
    daily_df = pd.read_json(daily_data_json)
    if "티커" in daily_df.columns:
        daily_df = daily_df.set_index("티커")
    return run_full_screening(daily_df, date)


# --- 데이터 조합 ---
def build_daily_data(date: str, mkt: str, days: int) -> pd.DataFrame:
    """시세 + 수급 + 섹터를 결합한 종합 데이터 빌드."""
    ohlcv = load_ohlcv(date, mkt)
    if ohlcv.empty:
        return pd.DataFrame()

    tickers_df = load_tickers(date, mkt).set_index("티커")
    ohlcv = ohlcv.join(tickers_df[["종목명"]], how="left")

    supply = load_supply(date, days, mkt)
    if not supply.empty:
        supply.columns = [f"{c}_5일" for c in supply.columns]
        ohlcv = ohlcv.join(supply, how="left")

    sectors = load_sectors(date, mkt)
    if not sectors.empty:
        ohlcv = ohlcv.join(sectors, how="left")

    fill_cols = [c for c in ohlcv.columns if "5일" in c]
    ohlcv[fill_cols] = ohlcv[fill_cols].fillna(0)

    return ohlcv


# 로드 버튼
if st.sidebar.button("🚀 데이터 로드 & 분석 시작", use_container_width=True, type="primary"):
    st.session_state["load_data"] = True

if st.session_state.get("load_data"):
    daily_df = build_daily_data(date_str, market, supply_days)

    if daily_df.empty:
        st.error("❌ 데이터를 가져올 수 없습니다. 날짜와 시장을 확인해주세요.")
        st.stop()

    # 자동 스냅샷 저장 (수급 데이터 장기 누적용)
    try:
        save_daily_snapshot(date_str, market)
    except Exception:
        pass  # 스냅샷 저장 실패해도 대시보드 이용에 지장 없음

    st.session_state["daily_df"] = daily_df

    # ─── 기준일 표시 ───
    import datetime as _dt
    _WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]
    _d = _dt.datetime.strptime(date_str, "%Y%m%d")
    _wd = _WEEKDAY_KR[_d.weekday()]
    st.markdown(
        f'<div style="text-align:center; padding:8px 0; margin-bottom:8px;">'
        f'<span style="background:linear-gradient(90deg,#4f46e5,#7c3aed); '
        f'color:#fff; padding:6px 24px; border-radius:20px; font-size:1.05em; font-weight:600;">'
        f'📅 데이터 기준일: {_d.strftime("%Y-%m-%d")} ({_wd})'
        f'</span></div>',
        unsafe_allow_html=True,
    )

    # 시장 요약
    st.markdown("---")
    summary_cols = st.columns(4)
    with summary_cols[0]:
        st.metric("📊 분석 종목 수", f"{len(daily_df):,}개")
    with summary_cols[1]:
        avg_change = daily_df["등락률"].mean()
        st.metric("📉 평균 등락률", f"{avg_change:+.2f}%")
    with summary_cols[2]:
        up_count = (daily_df["등락률"] > 0).sum()
        st.metric("🔴 상승 종목", f"{up_count:,}개")
    with summary_cols[3]:
        down_count = (daily_df["등락률"] < 0).sum()
        st.metric("🔵 하락 종목", f"{down_count:,}개")

    # ===================================================================
    # 탭 구성: 섹터 히트맵 / 상승 종목 분석 / 오늘의 발굴 종목 / 종목 상세
    # ===================================================================
    tab1, tab2, tab3, tab4 = st.tabs([
        "🗺️ 섹터 히트맵",
        "📊 상승 종목 분석",
        "🔥 오늘의 발굴 종목",
        "📈 종목 상세",
    ])

    # --- 탭 1: 섹터 히트맵 ---
    with tab1:
        render_sector_heatmap(daily_df)
        render_sector_bar_chart(daily_df)

    # --- 탭 2: 상승 종목 분석 (거래량, 추세, 분기 실적) ---
    with tab2:
        selected_from_rising = render_rising_stocks(daily_df)
        if selected_from_rising:
            st.session_state["selected_ticker"] = selected_from_rising

    # --- 탭 3: 오늘의 발굴 종목 ---
    with tab3:
        render_top_cards(daily_df, top_n=5)

        st.markdown("---")

        # 수급 필터링
        supply_filtered = screen_by_supply(daily_df)

        if not supply_filtered.empty:
            # 차트 + 기술적 지표 분석 (수급 TOP 종목 대상)
            with st.spinner("📊 차트 + 기술 지표 분석 중... (수급 상위 종목 대상)"):
                screened = add_chart_status(supply_filtered.head(top_n * 2), date_str)

            # 기술적 필터 적용 (차트상태 + RSI + MACD + 볼린저 + 거래량)
            screened = apply_technical_filters(
                screened,
                chart_filter=chart_filter,
                rsi_filter=rsi_filter,
                macd_filter=macd_filter,
                bb_filter=bb_filter,
                volume_surge_only=volume_surge_only,
            )

            selected_ticker = render_screened_table(screened, top_n)

            if selected_ticker:
                st.session_state["selected_ticker"] = selected_ticker
        else:
            st.info("기관/외국인 쌍끌이 순매수 종목이 없습니다.")

    # --- 탭 4: 종목 상세 ---
    with tab4:
        # 직접 입력 또는 테이블에서 선택
        col_a, col_b = st.columns([1, 2])
        with col_a:
            manual_ticker = st.text_input(
                "티커 직접 입력 (예: 005930)",
                value=st.session_state.get("selected_ticker", ""),
            )
        with col_b:
            st.markdown("<br>", unsafe_allow_html=True)
            if manual_ticker:
                st.info(f"선택된 종목: {manual_ticker}")

        detail_ticker = manual_ticker or st.session_state.get("selected_ticker")

        if detail_ticker:
            render_detail_view(detail_ticker, date_str)
        else:
            st.info(
                "💡 '오늘의 발굴 종목' 탭에서 종목을 선택하거나, "
                "위 입력란에 티커를 직접 입력해주세요."
            )

else:
    # 초기 안내 화면
    st.markdown("---")
    st.markdown(
        """
        <div style="text-align:center; padding:60px 20px;">
            <div style="font-size:4em; margin-bottom:20px;">🚀</div>
            <h2 style="color:#4f46e5;">시작하기</h2>
            <p style="color:#64748b; font-size:1.2em; max-width:600px; margin:0 auto;">
                왼쪽 사이드바에서 날짜와 시장을 설정한 후<br>
                <b>"데이터 로드 & 분석 시작"</b> 버튼을 클릭하세요.
            </p>
            <br>
            <div style="color:#94a3b8; font-size:0.9em;">
                🗺️ 섹터 히트맵 &nbsp;|&nbsp;
                📊 상승 종목 거래량·실적 &nbsp;|&nbsp;
                🔥 수급 기반 종목 발굴 &nbsp;|&nbsp;
                📈 상세 차트 분석
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
