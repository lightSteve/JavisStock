"""
📋 공통 종목 상세 컴포넌트 (with 메모 · 매매 계획)
- 캔들차트 + 이동평균선 + 거래량
- 수급 누적 그래프
- 메모 / 매매 플랜 텍스트 영역
- 매매 유형 (A-E) 체크박스
"""

import datetime
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from data.fetcher import get_stock_name, get_stock_ohlcv_history, get_investor_trend_individual
from analysis.indicators import calc_moving_averages


_MEMO_KEY = "stock_memos"


def render_stock_detail_common(ticker: str, date_str: str, context: str = ""):
    """공통 종목 상세 + 메모/매매 계획.

    Args:
        ticker: 종목코드
        date_str: 기준일 (YYYYMMDD)
        context: 호출 컨텍스트 식별자 (UI key 충돌 회피)
    """
    key_prefix = f"sdc_{context}_{ticker}"

    name = get_stock_name(ticker)
    st.markdown(f"### {name} ({ticker})")

    # ── 1) 차트 ──
    _render_chart(ticker, date_str, key_prefix)

    # ── 2) 메모 / 매매 유형 / 플랜 ──
    _render_memo_section(ticker, name, key_prefix)


# ─────────────────────────────────────────────────────────────────────
def _render_chart(ticker: str, date_str: str, key_prefix: str):
    """캔들+MA+거래량+수급 차트."""
    end_dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    start_dt = end_dt - datetime.timedelta(days=200)

    with st.spinner("시세 로딩..."):
        ohlcv = get_stock_ohlcv_history(ticker, start_dt.strftime("%Y%m%d"), date_str)

    if ohlcv.empty:
        st.warning("시세 데이터를 가져올 수 없습니다.")
        return

    ohlcv = calc_moving_averages(ohlcv)
    ohlcv.index = pd.to_datetime(ohlcv.index)

    with st.spinner("수급 로딩..."):
        try:
            supply = get_investor_trend_individual(ticker)
        except Exception:
            supply = pd.DataFrame()

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.55, 0.20, 0.25],
        subplot_titles=("가격 차트", "거래량", "수급 누적"),
    )

    # 캔들
    fig.add_trace(go.Candlestick(
        x=ohlcv.index, open=ohlcv["시가"], high=ohlcv["고가"],
        low=ohlcv["저가"], close=ohlcv["종가"], name="가격",
        increasing_line_color="#dc2626", decreasing_line_color="#2563eb",
        increasing_fillcolor="#dc2626", decreasing_fillcolor="#2563eb",
    ), row=1, col=1)

    # MA
    for ma, color in [("MA5", "#ffeb3b"), ("MA20", "#ff9800"), ("MA60", "#4caf50"), ("MA120", "#9c27b0")]:
        if ma in ohlcv.columns:
            fig.add_trace(go.Scatter(
                x=ohlcv.index, y=ohlcv[ma], name=ma,
                line=dict(color=color, width=1.2), opacity=0.8,
            ), row=1, col=1)

    # 거래량
    vol_colors = ["#dc2626" if c >= o else "#2563eb"
                  for c, o in zip(ohlcv["종가"], ohlcv["시가"])]
    fig.add_trace(go.Bar(
        x=ohlcv.index, y=ohlcv["거래량"], name="거래량",
        marker_color=vol_colors, opacity=0.7,
    ), row=2, col=1)

    # 수급
    if not supply.empty:
        for col_name, color, label in [
            ("기관합계", "#1f77b4", "기관"),
            ("외국인합계", "#ff7f0e", "외국인"),
        ]:
            if col_name in supply.columns:
                cum = supply[col_name].cumsum() / 1e8
                fig.add_trace(go.Scatter(
                    x=supply.index, y=cum, name=f"{label} 누적",
                    line=dict(color=color, width=2), fill="tozeroy", opacity=0.6,
                ), row=3, col=1)

    fig.update_layout(
        height=750, xaxis_rangeslider_visible=False,
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=10, r=10, t=30, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

    # 요약 지표
    last = ohlcv.iloc[-1]
    cols = st.columns(4)
    with cols[0]:
        price = last["종가"]
        change = last.get("등락률", 0)
        st.metric("현재가", f"{price:,.0f}원", f"{change:+.2f}%")
    with cols[1]:
        vol = last["거래량"]
        avg_vol = ohlcv["거래량"].iloc[-21:-1].mean() if len(ohlcv) > 21 else ohlcv["거래량"].mean()
        ratio = (vol / avg_vol * 100) if avg_vol > 0 else 0
        st.metric("거래량", f"{vol:,.0f}", f"평균 대비 {ratio:.0f}%")
    with cols[2]:
        if not supply.empty and "기관합계" in supply.columns:
            inst = supply["기관합계"].tail(5).sum() / 1e8
            st.metric("기관 5일", f"{inst:+,.1f}억")
        else:
            st.metric("기관 5일", "N/A")
    with cols[3]:
        if not supply.empty and "외국인합계" in supply.columns:
            frgn = supply["외국인합계"].tail(5).sum() / 1e8
            st.metric("외국인 5일", f"{frgn:+,.1f}억")
        else:
            st.metric("외국인 5일", "N/A")


# ─────────────────────────────────────────────────────────────────────
def _render_memo_section(ticker: str, name: str, key_prefix: str):
    """메모 · 매매 유형 · 매매 플랜."""
    st.markdown("#### 📝 메모 & 매매 플랜")

    if _MEMO_KEY not in st.session_state:
        st.session_state[_MEMO_KEY] = {}

    memo_data = st.session_state[_MEMO_KEY].get(ticker, {})

    c1, c2 = st.columns([2, 1])
    with c1:
        memo_text = st.text_area(
            "메모 / 매매 플랜",
            value=memo_data.get("memo", ""),
            height=100,
            key=f"{key_prefix}_memo",
            placeholder="매수 근거, 손절 기준, 목표가 등...",
        )
    with c2:
        st.markdown("**매매 유형**")
        types = memo_data.get("types", [])
        type_a = st.checkbox("A: 테마추격", value="A" in types, key=f"{key_prefix}_ta")
        type_b = st.checkbox("B: 뉴스스파이크", value="B" in types, key=f"{key_prefix}_tb")
        type_c = st.checkbox("C: 돌파매매", value="C" in types, key=f"{key_prefix}_tc")
        type_d = st.checkbox("D: 바이오회복", value="D" in types, key=f"{key_prefix}_td")
        type_e = st.checkbox("E: 스윙", value="E" in types, key=f"{key_prefix}_te")

    if st.button("💾 저장", key=f"{key_prefix}_save", type="primary"):
        checked = []
        if type_a: checked.append("A")
        if type_b: checked.append("B")
        if type_c: checked.append("C")
        if type_d: checked.append("D")
        if type_e: checked.append("E")

        st.session_state[_MEMO_KEY][ticker] = {
            "memo": memo_text,
            "types": checked,
            "name": name,
        }
        st.success(f"✅ {name} ({ticker}) 메모가 저장되었습니다.")
