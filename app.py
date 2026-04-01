import streamlit as st

# 빌드 버전 (Streamlit Cloud 캐시 클리어용)
# Build: 20260401-v2.1
# 환율 반영 실정 지수 기능에 필요한 패키지
import plotly.graph_objects as go



# ===================== USD/KRW 환율 전용 심플 대시보드 =====================
import yfinance as yf
import numpy as np
import pandas as pd
import plotly.express as px

@st.cache_data(ttl=3600)
def get_usdkrw():
    df = yf.download('KRW=X', period='1y', progress=False)
    df.index = pd.to_datetime(df.index).tz_localize(None)
    return df



# 1. 데이터 수집: 환율, S&P500, KOSPI
raw_usdkrw = get_usdkrw().dropna()
if isinstance(raw_usdkrw.columns, pd.MultiIndex):
    raw_usdkrw.columns = raw_usdkrw.columns.get_level_values(0)
usdkrw_df = raw_usdkrw[['Close']].copy()

spx_df = yf.download('^GSPC', period='1y', progress=False)
if isinstance(spx_df.columns, pd.MultiIndex):
    spx_df.columns = spx_df.columns.get_level_values(0)
spx_df = spx_df[['Close']].copy()

kospi_df = yf.download('^KS11', period='1y', progress=False)
if isinstance(kospi_df.columns, pd.MultiIndex):
    kospi_df.columns = kospi_df.columns.get_level_values(0)
kospi_df = kospi_df[['Close']].copy()


# 2. 날짜 기준 merge (공통 날짜만) 및 컬럼명 명확화
merged = usdkrw_df.rename(columns={'Close': 'Close_usdkrw'})
merged = merged.join(spx_df.rename(columns={'Close': 'Close_spx'}), how='inner')
merged = merged.join(kospi_df.rename(columns={'Close': 'Close_kospi'}), how='inner')

merged = merged.dropna()

# 데이터가 비었는지 체크
if merged.empty:
    st.error("공통 날짜에 데이터가 없습니다. 데이터 소스를 확인하세요.")
    st.stop()


# 3. 환산 지수 계산 (공식 명확화)
# S&P500(원화환산): 미국 S&P500 × 현재 환율 (환율이 오르면 더 위)
merged['KRW_S&P500'] = merged['Close_spx'] * merged['Close_usdkrw']
# KOSPI(달러환산): 한국 KOSPI ÷ 현재 환율 (환율이 오르면 더 아래)
merged['USD_KOSPI'] = merged['Close_kospi'] / merged['Close_usdkrw']


# 4. 정규화 (첫날 기준)
for col in ['Close_spx', 'KRW_S&P500', 'Close_kospi', 'USD_KOSPI']:
    merged[col + '_norm'] = merged[col] / merged[col].iloc[0]

# 5. Dual Chart 시각화 및 Gap 강조
st.markdown("## 환율 반영 실정 지수 비교")
import plotly.graph_objects as go


# --- 차트 색상만 명확하게, 선 두께 기본값(2)로 가독성 개선 ---
fig = go.Figure()
# KOSPI(달러환산) - 빨간 점선
fig.add_trace(go.Scatter(
    x=merged.index, y=merged['USD_KOSPI_norm'],
    mode='lines', name='KOSPI(달러환산)',
    line=dict(color='#e11d48', width=2, dash='dot'),
))
# KOSPI(KRW) - 진한 초록 실선
fig.add_trace(go.Scatter(
    x=merged.index, y=merged['Close_kospi_norm'],
    mode='lines', name='KOSPI(KRW)',
    line=dict(color='#065c2f', width=2),
))
# S&P500(원화환산) - 오렌지 점선
fig.add_trace(go.Scatter(
    x=merged.index, y=merged['KRW_S&P500_norm'],
    mode='lines', name='S&P500(원화환산)',
    line=dict(color='#f59e42', width=2, dash='dash'),
))
# S&P500(USD) - 파란 실선
fig.add_trace(go.Scatter(
    x=merged.index, y=merged['Close_spx_norm'],
    mode='lines', name='S&P500(USD)',
    line=dict(color='#2563eb', width=2),
))
# Y=1.0 기준선(회색)
fig.add_shape(type='line', x0=merged.index[0], x1=merged.index[-1], y0=1.0, y1=1.0,
              line=dict(color='#888', width=2, dash='dot'))

fig.update_layout(
    title='S&P500/KOSPI 환율 반영 실정 지수 (정규화)',
    xaxis_title='날짜',
    yaxis_title='정규화 지수(1.0=기준일)',
    height=480,
    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
    margin=dict(t=60, l=10, r=10, b=10),
    hovermode='x unified',
    plot_bgcolor='#fff',
)
st.plotly_chart(fig, width="stretch")

# 6. Gap 분석 리포트
st.markdown("### 환율 효과 분석 리포트")
gap_spx = merged['KRW_S&P500_norm'].iloc[-1] - merged['Close_spx_norm'].iloc[-1]
gap_kospi = merged['USD_KOSPI_norm'].iloc[-1] - merged['Close_kospi_norm'].iloc[-1]
if gap_spx > 0.01:
    st.success(f"최근 1년간 환율 상승으로 S&P500의 원화 기준 수익률이 미국 달러 기준보다 **{gap_spx*100:.2f}% 더 높게** 방어되고 있습니다. (Boost)")
elif gap_spx < -0.01:
    st.error(f"최근 1년간 환율 하락으로 S&P500의 원화 기준 수익률이 미국 달러 기준보다 **{abs(gap_spx)*100:.2f}% 더 낮게** 깎이고 있습니다. (Drag)")
else:
    st.info("최근 1년간 환율 변화가 S&P500의 원화 기준 수익률에 큰 영향을 주지 않았습니다.")

if gap_kospi > 0.01:
    st.success(f"최근 1년간 환율 하락으로 KOSPI의 달러 기준 수익률이 원화 기준보다 **{gap_kospi*100:.2f}% 더 높게** 방어되고 있습니다. (Boost)")
elif gap_kospi < -0.01:
    st.error(f"최근 1년간 환율 상승으로 KOSPI의 달러 기준 수익률이 원화 기준보다 **{abs(gap_kospi)*100:.2f}% 더 낮게** 깎이고 있습니다. (Drag)")
else:
    st.info("최근 1년간 환율 변화가 KOSPI의 달러 기준 수익률에 큰 영향을 주지 않았습니다.")

if usdkrw_df.empty or 'Close' not in usdkrw_df.columns:
    st.error("USD/KRW 환율 데이터가 없습니다. 네트워크 또는 API 오류일 수 있습니다.")
else:
    def safe_float(val):
        try:
            if isinstance(val, (float, int)):
                return float(val)
            if hasattr(val, 'item'):
                return float(val.item())
            return float(val)
        except Exception:
            return 0.0

    last = safe_float(usdkrw_df['Close'].iloc[-1])
    prev = safe_float(usdkrw_df['Close'].iloc[-2]) if len(usdkrw_df) > 1 else last
    diff = float(last - prev)
    diff_pct = float((diff / prev * 100) if prev != 0 and not np.isnan(prev) else 0.0)
    st.metric("USD/KRW 환율", f"{last:,.2f}", f"{diff:+.2f} ({diff_pct:+.2f}%)")

    # 최근 6개월~1년치 환율 추세선
    fig = px.line(
        usdkrw_df.reset_index(),
        x='Date', y='Close',
        title='USD/KRW 환율 추이 (최근 1년)',
        labels={'Close': '환율', 'Date': '날짜'},
        height=380
    )
    fig.update_traces(line_color="#2563eb", line_width=2)
    fig.update_layout(margin=dict(t=40, l=10, r=10, b=10))
    st.plotly_chart(fig, width="stretch")
from data.fetcher import get_market_mode, is_market_open, is_market_closed
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
    REFRESH_INTERVAL_SEC,
)
from analysis.screening import screen_by_supply, add_chart_status, run_full_screening, apply_technical_filters
from components.sidebar import render_sidebar
from components.heatmap import render_sector_heatmap, render_sector_bar_chart
from components.top_picks import render_top_cards, render_screened_table
from components.detail import render_detail_view
from components.supply_flow import render_supply_flow
from components.watchlist import render_watchlist_section

from components.rising_stocks import render_rising_stocks
from components.smart_picks import render_smart_top3
from components.market_regime import render_market_regime
from components.pair_trading import render_pair_trading
from components.smart_money import render_smart_money
from components.event_radar import render_event_radar
from components.us_market_banner import render_us_market_banner

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
from components.my_portfolio import render_my_portfolio
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

    /* ── 사이드바 (Material Design) ── */
    section[data-testid="stSidebar"] {
        background-color: #f5f7fa;
        color: #1e293b;
    }
    section[data-testid="stSidebar"] .stMarkdown,
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] .stSlider label,
    section[data-testid="stSidebar"] span {
        color: #1e293b !important;
    }
    section[data-testid="stSidebar"] h3 {
        color: #1e293b !important;
        font-weight: 700 !important;
        font-size: 1.1em !important;
        margin-bottom: 16px !important;
    }
    section[data-testid="stSidebar"] .stTextInput input,
    section[data-testid="stSidebar"] .stSelectbox select {
        background-color: #ffffff !important;
        color: #1e293b !important;
        border: 1px solid #e2e8f0 !important;
    }
    section[data-testid="stSidebar"] button {
        color: #1e293b !important;
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

# ── 미국 증시 배너 ──
render_us_market_banner()

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

# ── 자동 화면 새로고침 (장중에만) ──
# 스케줄러가 _store를 갱신해도 Streamlit은 사용자 인터랙션 없이 자동 재실행하지 않으므로
# st_autorefresh로 주기적 rerun을 발생시켜 갱신된 데이터를 화면에 반영한다.
try:
    from streamlit_autorefresh import st_autorefresh
    _status_for_refresh = get_data_status()
    if _status_for_refresh.get("is_market_hours", False):
        st_autorefresh(interval=REFRESH_INTERVAL_SEC * 1000, key="sched_autorefresh")
except ImportError:
    pass  # 패키지 미설치 시 무시 (수동 새로고침으로 동작)

# ===========================================================================
# 로드 버튼 (사이드바 상단에 배치됨) — 캐시 함수 정의 후 처리
# ===========================================================================

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


# 장중/마감 모드별 고정 캐시 함수
@st.cache_data(ttl=60, show_spinner="📡 실시간 데이터 로딩 중...")
def load_daily_data_open(date: str, mkt: str, days: int) -> pd.DataFrame:
    return smart_load_daily_data(date, mkt, days, force_refresh=True)


@st.cache_data(ttl=3600, show_spinner="📡 마감 데이터 로딩 중... (스냅샷)")
def load_daily_data_closed(date: str, mkt: str, days: int) -> pd.DataFrame:
    return smart_load_daily_data(date, mkt, days, force_refresh=False)


def load_daily_data_dynamic(date: str, mkt: str, days: int) -> pd.DataFrame:
    """모드에 따라 캐시된 로더 함수 선택."""
    import datetime as _dt
    now = _dt.datetime.now()
    mode = get_market_mode(now)
    if mode == 'open':
        return load_daily_data_open(date, mkt, days)
    else:
        return load_daily_data_closed(date, mkt, days)


@st.cache_data(ttl=3600, show_spinner="🔍 스크리닝 실행 중...")
def run_screening(daily_data_json: str, date: str):
    """캐시를 위해 JSON 직렬화된 데이터를 받아 스크리닝."""
    daily_df = pd.read_json(daily_data_json)
    if "티커" in daily_df.columns:
        daily_df = daily_df.set_index("티커")
    return run_full_screening(daily_df, date)


# 캐시 함수 정의 완료 — 이제 로드 버튼 처리 가능
# 로그인된 사용자는 자동으로 데이터 로드 시작
from components.auth import is_logged_in
if is_logged_in():
    # 최초 로드 또는 상태 초기화
    if "load_data" not in st.session_state:
        st.session_state["load_data"] = True
        st.session_state["force_refresh"] = False

# ── 스케줄러 자동 갱신 감지: _store에 새 데이터가 있으면 daily_df 자동 교체 ──
# 장마감 후 종가 확정 데이터가 들어오면 버튼 재클릭 없이 화면 자동 최신화
_sched_df, _sched_date, _sched_market, _sched_time = get_cached_data()
_prev_refresh = st.session_state.get("_last_sched_refresh")
if (st.session_state.get("load_data")
        and _sched_df is not None and not _sched_df.empty
        and _sched_date == date_str and _sched_market == market
        and _sched_time is not None
        and _sched_time != _prev_refresh):
    # 새로운 갱신 감지 → session_state 교체 후 rerun
    st.session_state["daily_df"] = _sched_df
    st.session_state["_last_sched_refresh"] = _sched_time
    # load_daily_data_dynamic.clear()  # 동적 함수이므로 별도 clear 불필요
    run_screening.clear()
    st.rerun()


def load_daily_data_with_progress(date: str, mkt: str, days: int) -> pd.DataFrame:
    """단계별 진행도를 표시하면서 데이터 로드."""
    from data.fetcher import (
        get_market_ohlcv, get_all_tickers, get_accumulated_investor_trading,
        get_sector_info, get_latest_trading_date, smart_load_daily_data
    )

    # 진행도 표시
    progress_placeholder = st.empty()

    try:
        # 스냅샷 또는 API 데이터 로드 (장 마감시간 판별 포함)
        progress_placeholder.progress(10, text="① 데이터 소스 확인 중... (10%)")
        import time
        time.sleep(0.2)

        # 시세 데이터 로드
        progress_placeholder.progress(30, text="② 시세 데이터 로드 중... (30%)")
        time.sleep(0.1)

        # 수급 데이터 로드
        progress_placeholder.progress(60, text="③ 기관/외국인 수급 데이터 로드 중... (60%)")
        time.sleep(0.1)

        # 업종 정보 로드
        progress_placeholder.progress(85, text="④ 업종 정보 로드 중... (85%)")
        time.sleep(0.1)

        # 실제 데이터 로드 (위 단계들은 시각적 피드백용)
        daily_df = smart_load_daily_data(date, mkt, days)

        # 완료
        progress_placeholder.progress(100, text="✅ 데이터 로드 완료! (100%)")
        time.sleep(0.3)
        progress_placeholder.empty()

        return daily_df
    except Exception as e:
        st.error(f"❌ 데이터 로드 중 오류: {e}")
        progress_placeholder.empty()
        raise


if st.session_state.get("load_data"):

    # 스케줄러 상태 미리 확인
    _cached_df, _cached_date, _cached_market, _cached_time = get_cached_data()
    _has_scheduler_data = (
        _cached_df is not None and not _cached_df.empty
        and _cached_date == date_str and _cached_market == market
    )

    if not _has_scheduler_data:
        # 스케줄러에 데이터 없음 → API 직접 로드 (진행도 표시)
        cols = st.columns([1, 4, 1])
        with cols[1]:
            st.info("📡 **실시간 데이터 수집 중...** (약 1분 30초 소요)")
            st.caption("약 2,700개 종목 시세 + 기관/외국인 수급 데이터 로딩 중...")

        daily_df = load_daily_data_with_progress(date_str, market, supply_days)
    else:
        # 스케줄러 캐시 사용
        with st.spinner("캐시 데이터 로드 중..."):
            daily_df = _cached_df
            if st.session_state.get("_last_sched_refresh") is None and _cached_time:
                st.session_state["_last_sched_refresh"] = _cached_time

    if daily_df.empty:
        st.error("❌ 데이터를 가져올 수 없습니다. 날짜와 시장을 확인해주세요.")
        st.stop()

    st.session_state["daily_df"] = daily_df

    # 로딩 완료 메시지
    if not _has_scheduler_data:
        st.success("✅ 데이터 수집 완료! 분석을 시작합니다...")

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
    _search_options = [""] + (
        daily_df[daily_df['종목명'].notna()]
        .apply(lambda x: f"{x['종목명']} ({x.name})", axis=1)
        .tolist()
    )
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
            if st.button("📈 상세 보기", key="search_go", type="primary", width="stretch"):
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
        render_detail_view(
            _detail_ticker,
            date_str,
            market=str(daily_df.at[_detail_ticker, "시장"]) if (_detail_ticker in daily_df.index and "시장" in daily_df.columns) else "",
        )
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
                st.dataframe(_up_show, width="stretch", height=400)
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
                st.dataframe(_dn_show, width="stretch", height=400)
            else:
                st.info("하락 종목이 없습니다.")

    # ===================================================================
    # 탭 구성: 수급 / 섹터 / 상승종목 / 발굴 / 트레이더(5유형) / 보유종목 / 일지 / 관심종목 / 상세
    # ===================================================================
    tab0, tab1, tab2, tab3, tab_trader, tab_portfolio, tab_journal, tab_watchlist, tab4 = st.tabs([
        "🏛️ 수급",
        "🗺️ 섹터",
        "📊 상승종목",
        "🔥 발굴",
        "🎯 트레이더",
        "💼 보유종목",
        "📓 매매일지",
        "⭐ 관심종목",
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
        import copy as _copy
        from data.price_cache import price_cache as _pc

        # ── 재분석 버튼 ──────────────────────────────────────────────────
        _t3_col1, _t3_col2 = st.columns([8, 1])
        with _t3_col2:
            if st.button("🔄", help="최신 데이터로 재분석 + 현재가 갱신", key="refresh_smart_top3"):
                from data.scheduler import invalidate_analysis
                invalidate_analysis()
                st.rerun()

        _cached_top3 = get_cached_smart_top3(date_str)
        _cached_screened = get_cached_screened(date_str)

        # ── 추적 티커 수집 + 일괄 현재가 갱신 ─────────────────────────
        # price_cache.ensure_fresh() 가 TTL 이내 티커는 API 호출 생략 → 중복 방지
        _tracked: set = set()
        if _cached_top3:
            _tracked.update(r["ticker"] for r in _cached_top3)
        if not daily_df.empty and "기관합계_5일" in daily_df.columns and "외국인합계_5일" in daily_df.columns:
            _sup = daily_df["기관합계_5일"].fillna(0) + daily_df["외국인합계_5일"].fillna(0)
            _tracked.update(_sup.nlargest(top_n).index.tolist())
        if _cached_screened is not None and not _cached_screened.empty:
            _tracked.update(_cached_screened.index.tolist())

        if _pc.needs_refresh(list(_tracked)):
            with st.spinner("📡 현재가 갱신 중..."):
                _pc.ensure_fresh(list(_tracked))

        # ── daily_df 에 캐시 최신 가격 일괄 반영 ──────────────────────
        # 이 시점부터 daily_df 의 해당 티커 가격은 price_cache 기준으로 통일됩니다.
        _pc.apply_to_dataframe(daily_df, list(_tracked))
        st.session_state["daily_df"] = daily_df

        # 갱신 시각 표시
        _bulk_ts = _pc.last_bulk_updated()
        if _bulk_ts:
            st.caption(f"💹 현재가 마지막 갱신: {_bulk_ts.strftime('%H:%M')} "
                       f"({'장중 5분' if True else '장외 1시간'} TTL · KIS/Naver 자동 선택)")

        # ── AI 스마트 Top 3 ────────────────────────────────────────────
        # daily_df 가 최신 가격이므로, precomputed 의 price 도 캐시 값으로 보정
        _top3_for_display = _copy.deepcopy(_cached_top3) if _cached_top3 else []
        for _r in _top3_for_display:
            _info = _pc.get(_r["ticker"])
            if _info and _info.get("price", 0) > 0:
                _r["price"] = _info["price"]
                _r["change"] = _info["change_rate"]

        render_smart_top3(daily_df, date_str, precomputed=_top3_for_display or None)

        st.markdown("---")

        # ── 수급 TOP 카드 (daily_df 이미 최신 가격) ────────────────────
        render_top_cards(daily_df, top_n=5)

        st.markdown("---")

        # ── 발굴 테이블 ────────────────────────────────────────────────
        if _cached_screened is not None and not _cached_screened.empty:
            screened = _cached_screened.copy()
            st.caption("⚡ 기술적 지표: 사전 분석 데이터 사용")

            # price_cache 최신 가격 반영 (daily_df 와 동일한 소스)
            _pc.apply_to_dataframe(screened, _cached_screened.index.tolist())

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
            # 사전 분석 캐시 없으면 실시간 분석
            supply_filtered = screen_by_supply(daily_df)

            if _cached_top3:
                _top3_tickers = [r["ticker"] for r in _cached_top3]
                _missing = [
                    t for t in _top3_tickers
                    if t in daily_df.index and (supply_filtered.empty or t not in supply_filtered.index)
                ]
                if _missing:
                    _extra = daily_df.loc[_missing].copy()
                    if "수급합계_5일" not in _extra.columns:
                        _extra["수급합계_5일"] = (
                            _extra.get("기관합계_5일", 0).fillna(0)
                            + _extra.get("외국인합계_5일", 0).fillna(0)
                        )
                    supply_filtered = pd.concat([_extra, supply_filtered]) if not supply_filtered.empty else _extra

            if not supply_filtered.empty:
                with st.spinner("📊 차트 + 기술 지표 분석 중..."):
                    screened = add_chart_status(supply_filtered.head(top_n * 2), date_str)

                # price_cache 최신 가격 반영
                _pc.apply_to_dataframe(screened, screened.index.tolist())

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
            if st.button("기록", key="pnl_record", width="stretch"):
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

    # --- 탭 보유종목 ---
    with tab_portfolio:
        render_my_portfolio(daily_df, date_str)

    # --- 탭 매매일지 ---
    with tab_journal:
        render_trading_journal(daily_df, date_str)

    # --- 탭 관심종목 ---
    with tab_watchlist:
        render_watchlist_section(daily_df)

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
    # 로그인되지 않음 - 안내 화면
    from components.auth import is_logged_in
    if not is_logged_in():
        st.markdown("---")
        st.markdown(
            """
            <div style="text-align:center; padding:40px 16px 20px;">
                <div style="font-size:3em; margin-bottom:16px;">📊</div>
                <h2 style="color:#4f46e5; margin-bottom:8px;">TrendCatcher</h2>
                <p style="color:#64748b; font-size:1em; max-width:500px; margin:0 auto 24px;">
                    왼쪽 사이드바에서 로그인하세요.<br>
                    로그인 후 최신 데이터와 분석 결과를 자동으로 확인할 수 있습니다.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
