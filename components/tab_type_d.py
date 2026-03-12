"""
🧬 Type D: 바이오 루머 급락 → 해명 → 회복 패턴
- 바이오 종목 급락 스캐너
- 해명/공시 뉴스 연동
- 회복 구간 베팅 타점
"""

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from data.fetcher import (
    get_stock_news_list,
    get_stock_ohlcv_history,
    get_investor_trend_individual,
)
from logic_patterns import is_bio_stock, detect_bio_crash, calc_recovery_stats


def render_tab_type_d(daily_df: pd.DataFrame, date_str: str):
    """Type D 탭 렌더링: 바이오 급락 → 회복."""
    st.markdown("## 🧬 Type D: 바이오 급락 → 회복 패턴")
    st.caption("루머 급락 스캔 · 해명 공시 · 회복 베팅 구간")

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    tab_scan, tab_recover = st.tabs(["🔍 급락 스캐너", "📈 회복 베팅"])

    with tab_scan:
        _render_bio_scanner(daily_df, date_str)

    with tab_recover:
        _render_recovery_zone(daily_df, date_str)


# ─────────────────────────────────────────────────────────────────────
def _render_bio_scanner(daily_df: pd.DataFrame, date_str: str):
    """바이오 종목 급락 스캐너."""
    st.markdown("### 🧬 바이오 급락 스캐너")

    threshold = st.slider("급락 기준 (%)", -20.0, -3.0, -5.0, 0.5, key="bio_threshold")
    crash = detect_bio_crash(daily_df, threshold=threshold)

    bio_count = daily_df.apply(is_bio_stock, axis=1).sum()
    severe = crash[crash["등락률"] <= -10] if not crash.empty else pd.DataFrame()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("🧬 바이오 전체", f"{bio_count}개")
    with c2:
        st.metric("🔴 급락(-10%↓)", f"{len(severe)}개")
    with c3:
        st.metric("🟡 하락", f"{len(crash)}개")

    if crash.empty:
        st.success("✅ 현재 급락 중인 바이오 종목이 없습니다.")
        return

    for ticker, row in crash.iterrows():
        name = row.get("종목명", ticker)
        change = row.get("등락률", 0)
        price = row.get("종가", 0)
        tv = row.get("거래대금", 0) / 1e8

        if change <= -20:
            sev, sev_color = "🔴 심각", "#991b1b"
        elif change <= -10:
            sev, sev_color = "🟠 경계", "#c2410c"
        else:
            sev, sev_color = "🟡 주의", "#a16207"

        st.markdown(
            f'<div style="background:#fff; border-radius:10px; padding:10px 14px; '
            f'border-left:4px solid {sev_color}; margin-bottom:6px;">'
            f'<div style="display:flex; justify-content:space-between; align-items:center;">'
            f'<div>'
            f'<span style="font-weight:700;">{name}</span>'
            f'<span style="color:#94a3b8; font-size:0.78em; margin-left:6px;">{ticker}</span>'
            f'</div>'
            f'<span style="color:{sev_color}; font-weight:700;">{change:.1f}% {sev}</span>'
            f'</div>'
            f'<div style="font-size:0.78em; color:#64748b; margin-top:3px;">'
            f'{price:,.0f}원 · 거래대금 {tv:,.0f}억</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # 뉴스 확인 (인라인 expander)
        with st.expander(f"📰 {name} 뉴스 확인"):
            news = get_stock_news_list(ticker)
            if news:
                for item in news[:5]:
                    st.markdown(f"- {item.get('title', '')}")
            else:
                st.caption("최근 관련 뉴스 없음")


# ─────────────────────────────────────────────────────────────────────
def _render_recovery_zone(daily_df: pd.DataFrame, date_str: str):
    """급락 후 회복 베팅 구간 분석."""
    st.markdown("### 📈 회복 베팅 구간")
    st.caption("급락 바이오 종목 중 회복 신호가 보이는 종목")

    crash = detect_bio_crash(daily_df, threshold=-5.0)
    if crash.empty:
        st.info("급락 바이오 종목이 없어 회복 분석을 건너뜁니다.")
        return

    options = [""] + [
        f"{row.get('종목명', t)} ({t})"
        for t, row in crash.iterrows()
    ]
    selected = st.selectbox("종목 선택 (회복 분석)", options, key="type_d_recovery")

    if not selected:
        st.info("급락 종목을 선택하면 회복 통계와 수급 현황을 보여줍니다.")
        return

    ticker = selected.split("(")[-1].rstrip(")")

    import datetime
    end_dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    start_dt = end_dt - datetime.timedelta(days=120)

    with st.spinner("시세 데이터 로딩 중..."):
        ohlcv = get_stock_ohlcv_history(ticker, start_dt.strftime("%Y%m%d"), date_str)

    if ohlcv.empty:
        st.warning("시세 데이터를 가져올 수 없습니다.")
        return

    # 회복 통계
    stats = calc_recovery_stats(ohlcv)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("급락폭", f"{stats['crash_pct']:.1f}%")
    with c2:
        st.metric("경과일", f"{stats['days_since']}일")
    with c3:
        rec_color = "normal" if stats["recovery_pct"] > 0 else "inverse"
        st.metric("회복률", f"{stats['recovery_pct']:+.1f}%", delta_color=rec_color)

    # 차트
    import plotly.graph_objects as go
    ohlcv_indexed = ohlcv.copy()
    ohlcv_indexed.index = pd.to_datetime(ohlcv_indexed.index)

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=ohlcv_indexed.index,
        open=ohlcv_indexed["시가"],
        high=ohlcv_indexed["고가"],
        low=ohlcv_indexed["저가"],
        close=ohlcv_indexed["종가"],
        name="가격",
        increasing_line_color="#dc2626",
        decreasing_line_color="#2563eb",
    ))
    fig.update_layout(
        height=350, template="plotly_white",
        xaxis_rangeslider_visible=False,
        margin=dict(l=10, r=10, t=30, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

    # 수급 확인
    with st.spinner("수급 데이터 로딩 중..."):
        try:
            supply = get_investor_trend_individual(ticker)
        except Exception:
            supply = pd.DataFrame()

    if not supply.empty:
        st.markdown("#### 수급 현황 (최근 5일)")
        cols_to_show = [c for c in ["기관합계", "외국인합계", "개인"] if c in supply.columns]
        if cols_to_show:
            st.dataframe(
                (supply[cols_to_show].tail(5) / 1e8).round(1),
                use_container_width=True,
            )
