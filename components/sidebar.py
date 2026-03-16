"""
사이드바 컴포넌트
- 날짜 선택 (기준일 표시)
- 시장 선택 (KOSPI / KOSDAQ / ALL)
- 수급 분석 기간
- 차트·기술 지표 필터 (정배열, RSI, MACD, 볼린저, 골든크로스)
"""

import datetime
import streamlit as st

from components.auth import render_login_sidebar, is_logged_in, get_username

# 요일 한글 매핑
_WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]


def _get_default_date() -> datetime.date:
    """분석 기준일 기본값: 오늘 날짜 (주말이면 가장 최근 평일)."""
    dt = datetime.date.today()
    while dt.weekday() >= 5:
        dt -= datetime.timedelta(days=1)
    return dt


def render_sidebar() -> dict:
    """사이드바 위젯을 렌더링하고 선택값을 딕셔너리로 반환."""
    st.sidebar.markdown("## 🎛️ 설정")

    # --- 로그인 ---
    render_login_sidebar()

    # --- 날짜 선택 ---
    st.sidebar.markdown("### 📅 기준일")
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
        f"📅 **기준일**: {selected_date.strftime('%Y-%m-%d')} ({wd})"
    )

    # --- 시장 선택 ---
    st.sidebar.markdown("### 🏛️ 시장")
    market = st.sidebar.selectbox(
        "시장 선택",
        options=["ALL", "KOSPI", "KOSDAQ"],
        index=0,
        help="전체 / 코스피 / 코스닥",
    )

    # --- 수급 기간 ---
    st.sidebar.markdown("### 💰 수급 분석")
    supply_days = st.sidebar.slider(
        "수급 누적 기간 (거래일)",
        min_value=1,
        max_value=20,
        value=5,
        help="기관/외국인 순매수를 누적 합산할 거래일 수",
    )

    # ── 로드 버튼 (사이드바 상단, 핵심 설정 바로 아래) ──
    st.sidebar.markdown("---")
    _load_clicked = st.sidebar.button(
        "🚀 데이터 로드 & 분석 시작", use_container_width=True, type="primary"
    )

    # --- 상세 필터 (접이식) ---
    with st.sidebar.expander("🔬 상세 필터 (차트/기술지표)", expanded=False):
        chart_filter = st.multiselect(
            "📊 이동평균 정배열",
            options=["완전정배열", "정배열초기", "골든크로스"],
            default=["완전정배열", "정배열초기"],
            help="이동평균선 배열 상태로 필터링합니다.",
        )

        rsi_filter = st.multiselect(
            "RSI 상태",
            options=["과매도", "약세", "중립", "강세", "과매수"],
            default=[],
            help="RSI 30 이하=과매도, 70 이상=과매수",
        )

        macd_filter = st.multiselect(
            "MACD 상태",
            options=["매수신호", "상승강화", "상승둔화", "매도신호", "하락강화", "하락둔화"],
            default=[],
            help="MACD 히스토그램 기반 상태 필터",
        )

        bb_filter = st.multiselect(
            "볼린저밴드 상태",
            options=["하단돌파", "하단근접", "중간", "상단근접", "상단돌파"],
            default=[],
            help="볼린저밴드 %B 기반 위치 필터",
        )

        volume_surge = st.checkbox(
            "거래량 급증(200%↑) 종목만", value=False,
            help="최근 거래량이 20일 평균 대비 2배 이상인 종목만 표시",
        )

        top_n = st.slider("Top N 종목 표시", 5, 50, 20)

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        '<div style="background:linear-gradient(135deg,#312e81,#4f46e5); '
        'border-radius:12px; padding:14px 16px; margin-bottom:10px;">'
        '<div style="font-size:0.82em; font-weight:700; color:#c7d2fe; '
        'margin-bottom:6px;">📜 Steve의 투자 명언</div>'
        '<div style="font-size:0.78em; color:#e0e7ff; line-height:1.5;">'
        '똑같은 매매로 잃는 것은 실력 문제다.<br>'
        '자신에게 떳떳한 노력을 하고 있는가?</div></div>',
        unsafe_allow_html=True,
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "<div style='text-align:center; color:gray; font-size:0.8em;'>"
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
        "load_clicked": _load_clicked,
    }
