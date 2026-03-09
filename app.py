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
    smart_load_daily_data,
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

@st.cache_data(ttl=3600, show_spinner="📡 데이터 로딩 중... (스냅샷 있으면 즉시 로드)")
def load_daily_data_cached(date: str, mkt: str, days: int) -> pd.DataFrame:
    """스냅샷 우선 → 없으면 API fetch. st.cache_data 로 세션 내 재사용."""
    return smart_load_daily_data(date, mkt, days)


@st.cache_data(ttl=3600, show_spinner="🔍 스크리닝 실행 중...")
def run_screening(daily_data_json: str, date: str):
    """캐시를 위해 JSON 직렬화된 데이터를 받아 스크리닝."""
    daily_df = pd.read_json(daily_data_json)
    if "티커" in daily_df.columns:
        daily_df = daily_df.set_index("티커")
    return run_full_screening(daily_df, date)


# 로드 버튼
if st.sidebar.button("🚀 데이터 로드 & 분석 시작", use_container_width=True, type="primary"):
    load_daily_data_cached.clear()
    run_screening.clear()
    st.session_state["load_data"] = True

if st.session_state.get("load_data"):
    daily_df = load_daily_data_cached(date_str, market, supply_days)

    if daily_df.empty:
        st.error("❌ 데이터를 가져올 수 없습니다. 날짜와 시장을 확인해주세요.")
        st.stop()

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

    # ─── 종목 검색 ─────────────────────────────────────────────────
    _search_options = [""] + [
        f"{row['종목명']} ({ticker})" 
        for ticker, row in daily_df.iterrows() 
        if pd.notna(row.get('종목명', ''))
    ]
    search_col1, search_col2 = st.columns([3, 1])
    with search_col1:
        search_picked = st.selectbox(
            "🔍 종목 검색",
            options=_search_options,
            index=0,
            placeholder="종목명 또는 티커를 입력하세요...",
            key="stock_search",
        )
    with search_col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if search_picked:
            # 괄호 안의 티커 추출
            _search_ticker = search_picked.split("(")[-1].rstrip(")")
            if st.button("📈 상세 보기", key="search_go", type="primary", use_container_width=True):
                st.session_state["selected_ticker"] = _search_ticker
                st.session_state["go_detail_tab"] = True

    # 시장 요약
    st.markdown("---")
    up_stocks = daily_df[daily_df["등락률"] > 0].copy()
    down_stocks = daily_df[daily_df["등락률"] < 0].copy()
    up_count = len(up_stocks)
    down_count = len(down_stocks)

    summary_cols = st.columns(4)
    with summary_cols[0]:
        st.metric("📊 분석 종목 수", f"{len(daily_df):,}개")
    with summary_cols[1]:
        avg_change = daily_df["등락률"].mean()
        st.metric("📉 평균 등락률", f"{avg_change:+.2f}%")
    with summary_cols[2]:
        st.metric("🔴 상승 종목", f"{up_count:,}개")
    with summary_cols[3]:
        st.metric("🔵 하락 종목", f"{down_count:,}개")

    # ─── 상승 / 하락 종목 리스트 ────────────────────────────────────
    list_col1, list_col2 = st.columns(2)
    with list_col1:
        with st.expander(f"🔴 상승 종목 {up_count}개 보기", expanded=False):
            if not up_stocks.empty:
                _up_display = up_stocks.sort_values("등락률", ascending=False).head(100)
                _up_show = pd.DataFrame({
                    "종목명": _up_display["종목명"] if "종목명" in _up_display.columns else _up_display.index,
                    "종가": _up_display["종가"].apply(lambda x: f"{int(x):,}"),
                    "등락률": _up_display["등락률"].apply(lambda x: f"+{x:.2f}%"),
                    "거래량": _up_display["거래량"].apply(lambda x: f"{int(x):,}"),
                })
                _up_show.index = _up_display.index
                st.dataframe(_up_show, use_container_width=True, height=400)
            else:
                st.info("상승 종목이 없습니다.")
    with list_col2:
        with st.expander(f"🔵 하락 종목 {down_count}개 보기", expanded=False):
            if not down_stocks.empty:
                _dn_display = down_stocks.sort_values("등락률", ascending=True).head(100)
                _dn_show = pd.DataFrame({
                    "종목명": _dn_display["종목명"] if "종목명" in _dn_display.columns else _dn_display.index,
                    "종가": _dn_display["종가"].apply(lambda x: f"{int(x):,}"),
                    "등락률": _dn_display["등락률"].apply(lambda x: f"{x:.2f}%"),
                    "거래량": _dn_display["거래량"].apply(lambda x: f"{int(x):,}"),
                })
                _dn_show.index = _dn_display.index
                st.dataframe(_dn_show, use_container_width=True, height=400)
            else:
                st.info("하락 종목이 없습니다.")

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
