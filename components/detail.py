"""
종목 상세 뷰 (Detail View) 컴포넌트
- 캔들 차트 + 이동평균선
- 수급 누적 꺾은선 그래프 오버레이
"""

import datetime
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from data.fetcher import (
    get_stock_name, get_stock_ohlcv_history, get_investor_trend_individual,
    get_kis_stock_investor, is_kis_configured,
)
from analysis.indicators import calc_moving_averages


def render_detail_view(ticker: str, date_str: str):
    """
    개별 종목 상세 차트를 렌더링합니다.
    - 캔들 차트 + MA
    - 거래량 바 차트
    - 수급 누적 그래프
    """
    st.markdown("## 📈 종목 상세 분석")

    name = get_stock_name(ticker)
    st.markdown(f"### {name} ({ticker})")

    # --- 시세 데이터 가져오기 (최근 120거래일) ---
    end_dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    start_dt = end_dt - datetime.timedelta(days=200)
    start_str = start_dt.strftime("%Y%m%d")

    with st.spinner("시세 데이터 로딩 중..."):
        ohlcv = get_stock_ohlcv_history(ticker, start_str, date_str)

    if ohlcv.empty:
        st.warning("시세 데이터를 가져올 수 없습니다.")
        return

    # 이동평균선 계산
    ohlcv = calc_moving_averages(ohlcv)
    ohlcv.index = pd.to_datetime(ohlcv.index)

    # --- 수급 데이터 가져오기 (integration 엔드포인트, 최근 5거래일) ---
    with st.spinner("수급 데이터 로딩 중..."):
        try:
            supply = get_investor_trend_individual(ticker)
        except Exception:
            supply = pd.DataFrame()

    # ===========================
    # 캔들 차트 + MA + 거래량
    # ===========================
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.55, 0.2, 0.25],
        subplot_titles=("가격 차트", "거래량", "수급 누적 추이"),
    )

    # 캔들 차트
    fig.add_trace(
        go.Candlestick(
            x=ohlcv.index,
            open=ohlcv["시가"],
            high=ohlcv["고가"],
            low=ohlcv["저가"],
            close=ohlcv["종가"],
            name="가격",
            increasing_line_color="#dc2626",
            decreasing_line_color="#2563eb",
            increasing_fillcolor="#dc2626",
            decreasing_fillcolor="#2563eb",
        ),
        row=1, col=1,
    )

    # 이동평균선
    ma_colors = {"MA5": "#ffeb3b", "MA20": "#ff9800", "MA60": "#4caf50", "MA120": "#9c27b0"}
    for ma, color in ma_colors.items():
        if ma in ohlcv.columns:
            fig.add_trace(
                go.Scatter(
                    x=ohlcv.index, y=ohlcv[ma],
                    name=ma, line=dict(color=color, width=1.2),
                    opacity=0.8,
                ),
                row=1, col=1,
            )

    # 거래량 바 차트
    colors = ["#dc2626" if c >= o else "#2563eb"
              for c, o in zip(ohlcv["종가"], ohlcv["시가"])]
    fig.add_trace(
        go.Bar(
            x=ohlcv.index, y=ohlcv["거래량"],
            name="거래량",
            marker_color=colors,
            opacity=0.7,
        ),
        row=2, col=1,
    )

    # 수급 누적 그래프
    if not supply.empty:
        for col_name, color, label in [
            ("기관합계", "#1f77b4", "기관"),
            ("외국인합계", "#ff7f0e", "외국인"),
            ("개인", "#2ca02c", "개인"),
        ]:
            if col_name in supply.columns:
                cumulative = supply[col_name].cumsum() / 1e8  # 억원 단위
                fig.add_trace(
                    go.Scatter(
                        x=supply.index, y=cumulative,
                        name=f"{label} 누적",
                        line=dict(color=color, width=2),
                        fill="tozeroy" if col_name != "개인" else None,
                        opacity=0.6,
                    ),
                    row=3, col=1,
                )

    fig.update_layout(
        height=900,
        xaxis_rangeslider_visible=False,
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=10, r=10, t=30, b=10),
    )
    fig.update_yaxes(title_text="가격(원)", row=1, col=1)
    fig.update_yaxes(title_text="거래량", row=2, col=1)
    fig.update_yaxes(title_text="누적 수급(억)", row=3, col=1)

    st.plotly_chart(fig, use_container_width=True)

    # --- 요약 정보 카드 ---
    _render_summary_cards(ohlcv, supply)

    # --- 장중 실시간 수급 (KIS) ---
    _render_kis_realtime_investor(ticker)


def _render_summary_cards(ohlcv: pd.DataFrame, supply: pd.DataFrame):
    """종목 요약 지표 카드."""
    st.markdown("### 📊 요약 지표")

    last = ohlcv.iloc[-1]
    cols = st.columns(4)

    with cols[0]:
        price = last["종가"]
        change = last.get("등락률", 0)
        color = "#dc2626" if change > 0 else "#2563eb"
        st.metric("현재가", f"{price:,.0f}원", f"{change:+.2f}%")

    with cols[1]:
        vol = last["거래량"]
        avg_vol = ohlcv["거래량"].iloc[-21:-1].mean() if len(ohlcv) > 21 else ohlcv["거래량"].mean()
        vol_ratio = (vol / avg_vol * 100) if avg_vol > 0 else 0
        st.metric("거래량", f"{vol:,.0f}", f"평균 대비 {vol_ratio:.0f}%")

    with cols[2]:
        if not supply.empty and "기관합계" in supply.columns:
            inst_5d = supply["기관합계"].tail(5).sum() / 1e8
            st.metric("기관 5일 순매수", f"{inst_5d:+,.1f}억", help="Naver 확정 데이터 (전일까지)")
        else:
            st.metric("기관 5일 순매수", "N/A")

    with cols[3]:
        if not supply.empty and "외국인합계" in supply.columns:
            frgn_5d = supply["외국인합계"].tail(5).sum() / 1e8
            st.metric("외국인 5일 순매수", f"{frgn_5d:+,.1f}억", help="Naver 확정 데이터 (전일까지)")
        else:
            st.metric("외국인 5일 순매수", "N/A")


def _render_kis_realtime_investor(ticker: str):
    """KIS 장중 실시간 수급 표시 (설정된 경우에만)."""
    if not is_kis_configured():
        return

    with st.spinner("장중 수급 조회 중..."):
        kis = get_kis_stock_investor(ticker)

    st.markdown("---")
    st.markdown("### 📡 오늘 장중 수급 (KIS 실시간)")

    if not kis:
        st.caption("장중 수급 데이터를 가져올 수 없습니다. (장 마감 후이거나 API 오류)")
        return

    kc1, kc2, kc3 = st.columns(3)
    inst_v = kis.get("기관", 0.0)
    frgn_v = kis.get("외국인", 0.0)
    indv_v = kis.get("개인", 0.0)

    with kc1:
        delta_color = "normal" if inst_v >= 0 else "inverse"
        st.metric("🏛️ 기관 순매수", f"{inst_v:+.2f}억", delta=f"{inst_v:+.2f}억", delta_color=delta_color)
    with kc2:
        delta_color = "normal" if frgn_v >= 0 else "inverse"
        st.metric("🌍 외국인 순매수", f"{frgn_v:+.2f}억", delta=f"{frgn_v:+.2f}억", delta_color=delta_color)
    with kc3:
        delta_color = "normal" if indv_v >= 0 else "inverse"
        st.metric("👤 개인 순매수", f"{indv_v:+.2f}억", delta=f"{indv_v:+.2f}억", delta_color=delta_color)

    # 스마트머니 합산 신호
    smart = inst_v + frgn_v
    if smart > 0:
        signal = f"🔴 스마트머니 순매수 {smart:+.2f}억 (기관+외국인)"
    elif smart < 0:
        signal = f"🔵 스마트머니 순매도 {smart:+.2f}억 (기관+외국인)"
    else:
        signal = "⚪ 스마트머니 중립"
    st.caption(signal)
