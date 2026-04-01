"""
사이드바 컴포넌트
- 날짜 선택 (기준일 표시)
- 시장 선택 (KOSPI / KOSDAQ / ALL)
- 수급 분석 기간
- 차트·기술 지표 필터 (정배열, RSI, MACD, 볼린저, 골든크로스)
"""

import datetime
import streamlit as st
from components.auth import render_login_sidebar

# 요일 한글 매핑
_WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]


def _get_default_date() -> datetime.date:
    """분석 기준일 기본값: 오늘 날짜 (주말이면 가장 최근 평일)."""
    import datetime as _dt
    from data.fetcher import get_latest_trading_date
    # get_latest_trading_date()는 YYYYMMDD 문자열 반환
    date_str = get_latest_trading_date()
    dt = _dt.datetime.strptime(date_str, "%Y%m%d").date()
    return dt


def render_sidebar() -> dict:
    """사이드바 위젯을 렌더링하고 선택값을 딕셔너리로 반환."""
    render_login_sidebar()
    st.sidebar.markdown("---")
    st.sidebar.markdown("## 설정")

    # --- 날짜 선택 ---
    st.sidebar.markdown("### 기준일")
    default_date = _get_default_date()
    selected_date = st.sidebar.date_input(
        "분석 기준일",
        value=default_date,
        max_value=datetime.date.today(),
        help="장 마감 후 데이터가 반영됩니다.",
    )
    date_str = selected_date.strftime("%Y%m%d")

    # 기준일 표시
    wd = _WEEKDAY_KR[selected_date.weekday()]
    st.sidebar.info(
        f"**기준일**: {selected_date.strftime('%Y-%m-%d')} ({wd})"
    )

    # --- 시장 선택 ---
    st.sidebar.markdown("### 시장")
    market = st.sidebar.selectbox(
        "시장 선택",
        options=["ALL", "KOSPI", "KOSDAQ"],
        index=0,
        help="전체 / 코스피 / 코스닥",
    )

    # --- 수급 기간 ---
    st.sidebar.markdown("### 수급 분석")
    supply_days = st.sidebar.slider(
        "수급 누적 기간 (거래일)",
        min_value=1,
        max_value=20,
        value=5,
        help="기관/외국인 순매수를 누적 합산할 거래일 수",
    )

    # 상세 필터는 고정값으로 설정 (UI에서 숨김)
    chart_filter = ["완전정배열", "정배열초기"]  # 기본값
    rsi_filter = []  # 필터 없음
    macd_filter = []  # 필터 없음
    bb_filter = []  # 필터 없음
    volume_surge = False  # 거래량 급증만 필터링 안 함
    top_n = 20  # 기본값

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        '<div style="background:#e8f5e9; border-radius:10px; padding:14px 16px; '
        'margin-bottom:10px; border-left:4px solid #4f46e5;">'
        '<div style="font-size:0.82em; font-weight:700; color:#1e293b; '
        'margin-bottom:6px;">💡 투자 명언</div>'
        '<div style="font-size:0.78em; color:#1e293b; line-height:1.5;">'
        '똑같은 매매로 잃는 것은 실력 문제다.<br>'
        '자신에게 떳떳한 노력을 하고 있는가?</div></div>',
        unsafe_allow_html=True,
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "<div style='text-align:center; color:#64748b; font-size:0.8em;'>"
        "TrendCatcher v2.0<br>Powered by Naver Finance & Streamlit"
        "</div>",
        unsafe_allow_html=True,
    )

    return {
        "date": date_str,
        "market": market,
        "supply_days": supply_days,
        "chart_filter": chart_filter,
        "rsi_filter": rsi_filter,
        "macd_filter": macd_filter,
        "bb_filter": bb_filter,
        "volume_surge": volume_surge,
        "top_n": top_n,
    }
