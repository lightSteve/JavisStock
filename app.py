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
    get_index_ohlcv,
)
from data.scheduler import (
    start_scheduler,
    get_cached_data,
    get_data_status,
    is_refreshing,
    get_cached_smart_top3,
    get_cached_screened,
    is_analysis_ready,
)
from analysis.screening import screen_by_supply, add_chart_status, run_full_screening, apply_technical_filters
from components.sidebar import render_sidebar
from components.heatmap import render_sector_heatmap, render_sector_bar_chart
from components.top_picks import render_top_cards, render_screened_table
from components.detail import render_detail_view
from components.supply_flow import render_supply_flow

from components.rising_stocks import render_rising_stocks
from components.smart_picks import render_smart_top3
from components.market_regime import render_market_regime
from components.pair_trading import render_pair_trading
from components.smart_money import render_smart_money
from components.event_radar import render_event_radar

# ── 신규: 5-Type 트레이더 탭 + 공통 상세 + 매매일지 ──
from components.tab_type_a import render_tab_type_a
from components.tab_type_b import render_tab_type_b
from components.tab_type_c import render_tab_type_c
from components.tab_type_d import render_tab_type_d
from components.tab_type_e import render_tab_type_e
from components.stock_detail_common import render_stock_detail_common
from components.trading_journal import render_trading_journal
from components.strategy_picks import render_strategy_picks
from components.knee_stocks import render_knee_stocks
from logic_market_regime import calc_market_regime, suggest_position_size, check_market_rest_signal

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
    /* ── 기본 초기화 ── */
    * { -webkit-tap-highlight-color: transparent; box-sizing: border-box; }

    /* ── 앱 배경 ── */
    .stApp { background-color: #f8f9fc; color: #1a1a2e; }

    /* ── 사이드바 ── */
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

    /* ── 헤더 ── */
    .main-header {
        background: linear-gradient(90deg, #4f46e5 0%, #7c3aed 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 1.9rem;
        font-weight: 800;
        text-align: center;
        padding: 6px 0;
        line-height: 1.2;
    }
    .sub-header {
        text-align: center;
        color: #64748b;
        font-size: 0.9em;
        margin-bottom: 12px;
        padding: 0 12px;
    }

    /* ── 탭 (가로 스크롤 + 폰트 축소) ── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background-color: #f1f5f9;
        border-radius: 12px;
        padding: 4px;
        overflow-x: auto !important;
        -webkit-overflow-scrolling: touch !important;
        scrollbar-width: none !important;
        flex-wrap: nowrap !important;
    }
    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar { display: none; }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 7px 14px;
        color: #475569;
        font-weight: 600;
        font-size: 0.88em;
        white-space: nowrap !important;
        flex-shrink: 0 !important;
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background-color: #fff;
        color: #4f46e5;
        box-shadow: 0 1px 3px rgba(0,0,0,0.10);
    }

    /* ── 메트릭 카드 ── */
    [data-testid="stMetric"] {
        background-color: #ffffff;
        border-radius: 12px;
        padding: 14px 16px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    [data-testid="stMetric"] label {
        color: #64748b !important;
        font-weight: 600;
        font-size: 0.78em !important;
    }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #1e293b !important;
        font-weight: 700;
        font-size: 1.25em !important;
    }

    /* ── 데이터프레임 ── */
    .stDataFrame { border-radius: 12px; overflow: hidden; }
    [data-testid="stDataFrameContainer"] { overflow-x: auto !important; }

    /* ── 마크다운 ── */
    .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 { color: #1e293b; }
    .stMarkdown p, .stMarkdown li { color: #334155; }

    /* ── 버튼 ── */
    .stButton button[kind="primary"] {
        background-color: #4f46e5;
        color: white;
        border: none;
        border-radius: 8px;
        font-weight: 600;
    }
    .stButton button {
        border-radius: 8px !important;
        font-weight: 600;
        font-size: 0.9em;
    }

    /* ── 알림 박스 ── */
    .stAlert { border-radius: 10px; }

    /* ── 인풋 / 셀렉트박스 ── */
    .stSelectbox > div > div { border-radius: 8px !important; }
    .stTextInput > div > div { border-radius: 8px !important; }

    /* ════════════════════════════════
       모바일 반응형 (max-width: 768px)
       ════════════════════════════════ */
    @media (max-width: 768px) {
        /* 헤더 축소 */
        .main-header { font-size: 1.4rem !important; }
        .sub-header  { font-size: 0.82em !important; }

        /* 컬럼 그리드: 최소 160px → 화면 너비에 따라 2열로 자동 래핑 */
        [data-testid="stHorizontalBlock"] {
            flex-wrap: wrap !important;
        }
        [data-testid="stHorizontalBlock"] > [data-testid="column"] {
            min-width: calc(50% - 0.5rem) !important;
            flex: 1 1 calc(50% - 0.5rem) !important;
        }

        /* 버튼 터치 영역 확대 */
        .stButton button {
            min-height: 44px !important;
            font-size: 0.95em !important;
        }

        /* 메트릭 글자 크기 조정 */
        [data-testid="stMetric"] { padding: 10px 12px; }
        [data-testid="stMetric"] [data-testid="stMetricValue"] {
            font-size: 1.1em !important;
        }

        /* 섹션 h2 축소 */
        .stMarkdown h2 { font-size: 1.15em !important; }
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
    '<div class="sub-header">전종목 우상향 &amp; 수급 분석 대시보드</div>',
    unsafe_allow_html=True,
)

# ── 21일의 법칙 ──
st.markdown(
    '<div style="background:linear-gradient(135deg,#fefce8,#fef9c3); '
    'border:1px solid #fde68a; border-radius:12px; padding:12px 18px; '
    'margin-bottom:14px;">'
    '<div style="font-weight:700; font-size:0.92em; color:#92400e; '
    'margin-bottom:8px;">🏆 수익을 만드는 21일의 법칙</div>'
    '<div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:10px;">'
    '<div style="background:#fff; border-radius:10px; padding:10px; '
    'border-left:3px solid #16a34a;">'
    '<div style="font-weight:700; color:#16a34a; font-size:0.82em;">🚫 안 하기</div>'
    '<div style="font-size:0.73em; color:#475569; margin-top:4px;">'
    '뇌동매매와 오기 부리기를<br>단호히 거부하라.</div></div>'
    '<div style="background:#fff; border-radius:10px; padding:10px; '
    'border-left:3px solid #2563eb;">'
    '<div style="font-weight:700; color:#2563eb; font-size:0.82em;">✅ 실행하기</div>'
    '<div style="font-size:0.73em; color:#475569; margin-top:4px;">'
    '복기와 시나리오 작성을<br>단 한 장이라도 매일 하라.</div></div>'
    '<div style="background:#fff; border-radius:10px; padding:10px; '
    'border-left:3px solid #7c3aed;">'
    '<div style="font-weight:700; color:#7c3aed; font-size:0.82em;">⏳ 기다리기</div>'
    '<div style="font-size:0.73em; color:#475569; margin-top:4px;">'
    '본전만 지켜도 기회는 온다.<br>조급함은 최대의 적이다.</div></div>'
    '</div>'
    '<div style="font-size:0.7em; color:#92400e; margin-top:8px; text-align:center;">'
    '⚠️ 지피지기 백전불태: 나의 약점을 기록하고, 그 약점을 하나씩 걷어내는 과정이 곧 수익이다.</div>'
    '</div>',
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
# 백그라운드 데이터 갱신 스케줄러 (10분 간격)
# ===========================================================================
start_scheduler(date_str, market, supply_days)

# ===========================================================================
# 로드 버튼 (사이드바 상단에 배치됨)
# ===========================================================================
if config.get("load_clicked"):
    load_daily_data_cached.clear()
    run_screening.clear()
    st.session_state["load_data"] = True

# 스케줄러 상태 표시 (사이드바)
_sched_status = get_data_status()
if _sched_status["has_data"]:
    _upd = _sched_status["updated_at"]
    _upd_str = _upd.strftime("%H:%M:%S") if _upd else "-"
    _indicator = "🟢" if not _sched_status["is_refreshing"] else "🟡 갱신중"
    _analysis_icon = "✅" if _sched_status.get("has_analysis") else "⏳"
    st.sidebar.caption(
        f"{_indicator} 자동갱신 | 최신: {_upd_str} | "
        f"{_sched_status['stock_count']:,}종목 | "
        f"{'장중' if _sched_status['is_market_hours'] else '장마감'} | "
        f"분석{_analysis_icon}"
    )
else:
    if _sched_status["is_refreshing"]:
        st.sidebar.caption("🟡 최초 데이터 준비 중...")

# ===========================================================================
# 데이터 로딩 (스케줄러 우선 → 캐시 → API 순)
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

if st.session_state.get("load_data"):
    # 스케줄러에 이미 데이터가 있으면 즉시 사용
    _cached_df, _cached_date, _cached_market, _cached_time = get_cached_data()
    if (_cached_df is not None and not _cached_df.empty
            and _cached_date == date_str and _cached_market == market):
        daily_df = _cached_df
    else:
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
            _search_ticker = search_picked.split("(")[-1].rstrip(")")
            if st.button("📈 상세 보기", key="search_go", type="primary", use_container_width=True):
                st.session_state["selected_ticker"] = _search_ticker
                st.session_state["show_search_detail"] = _search_ticker

    # ─── 검색 결과 상세 (인라인) ─────────────────────────────────
    _detail_ticker = st.session_state.get("show_search_detail")
    if _detail_ticker:
        st.markdown("---")
        _close_col1, _close_col2 = st.columns([6, 1])
        with _close_col2:
            if st.button("✖ 닫기", key="close_search_detail"):
                st.session_state.pop("show_search_detail", None)
                st.rerun()
        render_detail_view(_detail_ticker, date_str)
        st.markdown("---")

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

    # ─── 종합지수 기반 "쉬어가기" 신호 ──────────────────────────────
    _kospi_index = get_index_ohlcv("KOSPI", 120)
    _kosdaq_index = get_index_ohlcv("KOSDAQ", 120)
    _rest_kospi = check_market_rest_signal(_kospi_index)
    _rest_kosdaq = check_market_rest_signal(_kosdaq_index)

    # 둘 중 더 위험한 쪽 기준
    _rest_signals = []
    for _idx_name, _rs in [("KOSPI", _rest_kospi), ("KOSDAQ", _rest_kosdaq)]:
        if _rs["should_rest"] or _rs["caution"]:
            _rest_signals.append((_idx_name, _rs))

    if _rest_signals:
        _worst = max(_rest_signals, key=lambda x: x[1]["score"])
        _ws_name, _ws = _worst

        if _ws["should_rest"]:
            _banner_bg = "linear-gradient(135deg, #dc2626 0%, #991b1b 100%)"
            _banner_icon = "🛑"
            _banner_title = "오늘은 거래를 쉬어가세요"
            _banner_sub = "종합지수가 다수의 위험 신호를 보이고 있습니다. 무리한 매매는 큰 손실로 이어질 수 있습니다."
        else:
            _banner_bg = "linear-gradient(135deg, #d97706 0%, #92400e 100%)"
            _banner_icon = "⚠️"
            _banner_title = "시장 주의 구간"
            _banner_sub = "종합지수 흐름이 불안정합니다. 신규 진입 시 비중을 줄이고 신중하게 접근하세요."

        _detail_lines = ""
        for _idx_name, _rs in _rest_signals:
            _reasons_str = " / ".join(_rs["reasons"]) if _rs["reasons"] else "양호"
            _price_str = f'{_rs["current_price"]:,.1f}' if _rs["current_price"] else "N/A"
            _ma20_str = f'{_rs["ma20"]:,.1f}' if _rs["ma20"] else "-"
            _ma60_str = f'{_rs["ma60"]:,.1f}' if _rs["ma60"] else "-"
            _detail_lines += (
                f'<div style="margin-top:6px; font-size:0.85em;">'
                f'<b>{_idx_name}</b>: {_price_str} '
                f'(20MA: {_ma20_str} / 60MA: {_ma60_str}) '
                f'&mdash; {_reasons_str}'
                f'</div>'
            )

        st.markdown(
            f'<div style="background:{_banner_bg}; color:#fff; border-radius:14px; '
            f'padding:18px 24px; margin:12px 0; box-shadow:0 4px 12px rgba(0,0,0,0.15);">'
            f'<div style="font-size:1.6em; font-weight:800;">'
            f'{_banner_icon} {_banner_title}</div>'
            f'<div style="font-size:0.95em; margin-top:4px; opacity:0.95;">{_banner_sub}</div>'
            f'{_detail_lines}'
            f'<div style="margin-top:8px; font-size:0.75em; opacity:0.7;">'
            f'위험도 점수: {_ws["score"]}/100</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

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
    # 탭 구성: 수급 / 섹터 / 상승종목 / 발굴 / 트레이더(5유형) / 일지 / 상세
    # ===================================================================
    tab0, tab1, tab2, tab3, tab_trader, tab_journal, tab4 = st.tabs([
        "🏛️ 수급",
        "🗺️ 섹터",
        "📊 상승종목",
        "🔥 발굴",
        "🎯 트레이더",
        "📓 매매일지",
        "📈 상세",
    ])

    # --- 탭 0: 기관·외국인 수급 ---
    with tab0:
        selected_from_supply = render_supply_flow(daily_df)
        if selected_from_supply:
            st.session_state["selected_ticker"] = selected_from_supply

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
        # ── AI 스마트 Top 3 (멀티팩터 점수 기반) ──
        _t3_col1, _t3_col2 = st.columns([8, 1])
        with _t3_col2:
            if st.button("🔄", help="최신 데이터로 재분석", key="refresh_smart_top3"):
                from data.scheduler import invalidate_analysis
                invalidate_analysis()
                st.rerun()
        _cached_top3 = get_cached_smart_top3(date_str)
        render_smart_top3(daily_df, date_str, precomputed=_cached_top3 or None)

        st.markdown("---")

        # ── 수급 TOP 카드 (기존) ──
        render_top_cards(daily_df, top_n=5)

        st.markdown("---")

        # 수급 필터링 → 사전 분석 결과 우선 사용
        _cached_screened = get_cached_screened(date_str)
        if _cached_screened is not None and not _cached_screened.empty:
            screened = _cached_screened
            st.caption("⚡ 기술적 지표: 사전 분석 데이터 사용")

            # 기술적 필터 적용
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
            # 캐시 없으면 기존 방식으로 실시간 분석
            supply_filtered = screen_by_supply(daily_df)

            if not supply_filtered.empty:
                with st.spinner("📊 차트 + 기술 지표 분석 중... (수급 상위 종목 대상)"):
                    screened = add_chart_status(supply_filtered.head(top_n * 2), date_str)

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

        st.markdown("---")

        # ── 무릎 아래: 저평가 가치주 발굴 ──
        render_knee_stocks(daily_df, date_str)

    # --- 탭 트레이더: 5-Type 매매 유형별 대시보드 ---
    with tab_trader:
        # ─── 상단 시장 국면 요약 + PnL 비중 조절 ─────────────────
        regime = calc_market_regime(daily_df)
        pnl_history = st.session_state.get("pnl_history", [])

        sizing = suggest_position_size(regime, pnl_history)

        regime_col, sizing_col, pnl_col = st.columns([2, 2, 1])
        with regime_col:
            st.markdown(
                f'<div style="background:{regime["color"]}15; border:2px solid {regime["color"]}; '
                f'border-radius:12px; padding:12px; text-align:center;">'
                f'<div style="font-size:0.8em; color:#64748b;">시장 국면</div>'
                f'<div style="font-size:1.8em; font-weight:800; color:{regime["color"]};">'
                f'{regime["label"]}</div>'
                f'<div style="font-size:0.78em; color:#64748b;">'
                f'거래대금 {regime["total_tv_조"]:.1f}조 · 상승비율 {regime["up_ratio"]:.0f}% · '
                f'점수 {regime["score"]:.0f}/100</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        with sizing_col:
            st.markdown(
                f'<div style="background:#fff; border:2px solid {sizing["grade_color"]}; '
                f'border-radius:12px; padding:12px; text-align:center;">'
                f'<div style="font-size:0.8em; color:#64748b;">권장 배팅 비중</div>'
                f'<div style="font-size:2.2em; font-weight:800; color:{sizing["grade_color"]};">'
                f'{sizing["final_pct"]}%</div>'
                f'<div style="font-size:0.85em; font-weight:600; color:{sizing["grade_color"]};">'
                f'{sizing["grade"]}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        with pnl_col:
            st.markdown("**PnL 입력**")
            new_pnl = st.number_input("오늘 손익(%)", value=0.0, step=0.5, key="daily_pnl_input")
            if st.button("기록", key="pnl_record", use_container_width=True):
                if "pnl_history" not in st.session_state:
                    st.session_state["pnl_history"] = []
                st.session_state["pnl_history"].insert(0, new_pnl)
                st.rerun()

        if sizing.get("pnl_warning"):
            st.warning(sizing["pnl_warning"])

        st.markdown(f"<div style='font-size:0.78em; color:#64748b; text-align:center; margin:4px 0;'>"
                    f"📋 {sizing['reason']}</div>", unsafe_allow_html=True)

        st.markdown("---")

        # ─── 5-Type 탭 ─────────────────────────────────────────
        tt_a, tt_b, tt_c, tt_d, tt_e, tt_strat, tt_regime = st.tabs([
            "🏆 A:테마추격",
            "📰 B:뉴스스파이크",
            "📈 C:돌파매매",
            "📊 D:섹터회복",
            "⚡ E:스윙·포지션",
            "📋 전략추천",
            "📊 시장국면상세",
        ])
        with tt_a:
            render_tab_type_a(daily_df, date_str)
        with tt_b:
            render_tab_type_b(daily_df, date_str)
        with tt_c:
            render_tab_type_c(daily_df, date_str)
        with tt_d:
            render_tab_type_d(daily_df, date_str)
        with tt_e:
            render_tab_type_e(daily_df, date_str)
        with tt_strat:
            render_strategy_picks(daily_df, date_str)
        with tt_regime:
            render_market_regime(daily_df)

    # --- 탭 매매일지 ---
    with tab_journal:
        render_trading_journal(daily_df, date_str)

    # --- 탭 4: 종목 상세 (공통 상세 + 메모/플랜 포함) ---
    with tab4:
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
            # 공통 상세 (캔들+MA+수급+메모/매매유형 체크)
            render_stock_detail_common(detail_ticker, date_str, context="main_detail")
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
        <div style="text-align:center; padding:40px 16px 20px;">
            <div style="font-size:3em; margin-bottom:16px;">📊</div>
            <h2 style="color:#4f46e5; margin-bottom:8px;">TrendCatcher 시작하기</h2>
            <p style="color:#64748b; font-size:1em; max-width:500px; margin:0 auto 24px;">
                왼쪽 사이드바에서 날짜와 시장을 선택한 후<br>
                <b>"데이터 로드 & 분석 시작"</b> 버튼을 누르세요.
            </p>
        </div>
        <div style="display:flex; flex-wrap:wrap; gap:12px; justify-content:center; padding:0 16px 40px; max-width:700px; margin:0 auto;">
            <div style="flex:1 1 140px; background:#fff; border-radius:12px; padding:16px; border:1px solid #e2e8f0; text-align:center;">
                <div style="font-size:1.6em;">🏛️</div>
                <div style="font-weight:700; font-size:0.9em; color:#1e293b; margin-top:6px;">기관·외국인 수급</div>
                <div style="font-size:0.78em; color:#64748b; margin-top:4px;">쌍끌이 종목 포착</div>
            </div>
            <div style="flex:1 1 140px; background:#fff; border-radius:12px; padding:16px; border:1px solid #e2e8f0; text-align:center;">
                <div style="font-size:1.6em;">🗺️</div>
                <div style="font-weight:700; font-size:0.9em; color:#1e293b; margin-top:6px;">섹터 히트맵</div>
                <div style="font-size:0.78em; color:#64748b; margin-top:4px;">시가총액 비중 시각화</div>
            </div>
            <div style="flex:1 1 140px; background:#fff; border-radius:12px; padding:16px; border:1px solid #e2e8f0; text-align:center;">
                <div style="font-size:1.6em;">🔥</div>
                <div style="font-weight:700; font-size:0.9em; color:#1e293b; margin-top:6px;">AI 발굴 TOP 3</div>
                <div style="font-size:0.78em; color:#64748b; margin-top:4px;">멀티팩터 점수 분석</div>
            </div>
            <div style="flex:1 1 140px; background:#fff; border-radius:12px; padding:16px; border:1px solid #e2e8f0; text-align:center;">
                <div style="font-size:1.6em;">📈</div>
                <div style="font-weight:700; font-size:0.9em; color:#1e293b; margin-top:6px;">종목 상세 차트</div>
                <div style="font-size:0.78em; color:#64748b; margin-top:4px;">캔들 + 수급 + 실적</div>
            </div>
            <div style="flex:1 1 140px; background:#fff; border-radius:12px; padding:16px; border:1px solid #e2e8f0; text-align:center;">
                <div style="font-size:1.6em;">🎯</div>
                <div style="font-weight:700; font-size:0.9em; color:#1e293b; margin-top:6px;">트레이더 5-Type</div>
                <div style="font-size:0.78em; color:#64748b; margin-top:4px;">테마·뉴스·돌파·바이오·스윙</div>
            </div>
            <div style="flex:1 1 140px; background:#fff; border-radius:12px; padding:16px; border:1px solid #e2e8f0; text-align:center;">
                <div style="font-size:1.6em;">📓</div>
                <div style="font-weight:700; font-size:0.9em; color:#1e293b; margin-top:6px;">매매 일지</div>
                <div style="font-size:0.78em; color:#64748b; margin-top:4px;">기록·복기·유형별통계</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
