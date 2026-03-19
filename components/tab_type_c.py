"""
📈 Type C: 전고/신고가 돌파 매매
- 전고점(20일) 돌파 후보
- 프로그램 순매수 + 돌파 필터
- 기관·외국인 수급 동반 확인
"""

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from data.fetcher import (
    get_program_trading_top,
    get_stock_ohlcv_history,
    detect_volume_spike_stocks,
)
from logic_patterns import detect_breakout_candidates, check_52week_high, check_swing_breakout
from components.watchlist import get_watchlist, add_to_watchlist, remove_from_watchlist


def render_tab_type_c(daily_df: pd.DataFrame, date_str: str):
    """Type C 탭 렌더링: 전고/신고가 돌파 매매."""
    st.markdown("## 📈 Type C: 전고 · 신고가 돌파")
    st.caption("프로그램 순매수 + 박스권 돌파 + 기관·외국인 수급 동반")

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    # ─── 1) 프로그램 순매수 추적 ───────────────────────────────
    _render_program_flow(daily_df, date_str)

    st.markdown("---")

    # ─── 2) 돌파 후보 종목 ────────────────────────────────────
    _render_breakout_list(daily_df, date_str)


# ─────────────────────────────────────────────────────────────────────
def _render_program_flow(daily_df: pd.DataFrame, date_str: str):
    """프로그램 순매수 상위 종목."""
    st.markdown("### 📊 프로그램 순매수 추적기")

    with st.spinner("프로그램 매매 데이터 수집 중..."):
        prog_df = get_program_trading_top()

    if prog_df.empty:
        st.caption("⚠️ 프로그램 데이터 수집 불가 — 기관 순매수 기반 대체.")
        _render_inst_proxy(daily_df)
        return

    buy_df = prog_df[prog_df["프로그램순매수"] > 0].sort_values("프로그램순매수", ascending=False).head(15)
    if buy_df.empty:
        st.info("프로그램 순매수 종목이 없습니다.")
        return

    fig = go.Figure(go.Bar(
        y=buy_df["종목명"][::-1],
        x=buy_df["프로그램순매수"][::-1],
        orientation="h",
        marker_color="#4f46e5",
        text=buy_df["프로그램순매수"][::-1].apply(lambda x: f"{x:,.0f}"),
        textposition="outside",
    ))
    fig.update_layout(
        title="프로그램 순매수 TOP 15",
        height=400,
        margin=dict(l=10, r=60, t=40, b=10),
        template="plotly_white",
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_inst_proxy(daily_df: pd.DataFrame):
    """기관 수급 기반 대체."""
    if "기관합계_5일" not in daily_df.columns:
        st.info("수급 데이터가 없습니다.")
        return

    inst_top = daily_df[
        (daily_df["기관합계_5일"] > 0) & (daily_df["등락률"] > 0)
    ].nlargest(15, "기관합계_5일")

    if inst_top.empty:
        st.info("기관 순매수 + 양봉 종목이 없습니다.")
        return

    for ticker, row in inst_top.iterrows():
        name = row.get("종목명", ticker)
        change = row.get("등락률", 0)
        inst = row["기관합계_5일"] / 1e8
        frgn = row.get("외국인합계_5일", 0) / 1e8
        chg_color = "#dc2626" if change > 0 else "#2563eb"
        st.markdown(
            f'<div style="display:flex; justify-content:space-between; '
            f'padding:6px 0; border-bottom:1px solid #f1f5f9; font-size:0.88em;">'
            f'<span>{name} ({ticker})</span>'
            f'<span style="color:{chg_color};">{change:+.1f}%</span>'
            f'<span style="color:#4f46e5;">기관 {inst:+,.0f}억 · 외인 {frgn:+,.0f}억</span>'
            f'</div>',
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────────────────────────────────
def _render_breakout_list(daily_df: pd.DataFrame, date_str: str):
    """전고점 돌파 후보."""
    st.markdown("### 🎯 돌파 후보 종목")

    breakout = detect_breakout_candidates(daily_df)
    if breakout.empty:
        st.info("돌파 후보 조건을 충족하는 종목이 없습니다.")
        return

    top = breakout.head(20)
    c1, c2 = st.columns(2)
    with c1:
        st.metric("돌파 후보", f"{len(breakout)}개")
    with c2:
        supply_both = 0
        if "기관합계_5일" in top.columns and "외국인합계_5일" in top.columns:
            supply_both = len(top[(top["기관합계_5일"] > 0) & (top["외국인합계_5일"] > 0)])
        st.metric("수급 동반", f"{supply_both}개")

    # 테이블
    display_cols = ["종목명", "등락률", "거래대금", "breakout_type"]
    if "기관합계_5일" in top.columns:
        display_cols.append("기관합계_5일")
    if "외국인합계_5일" in top.columns:
        display_cols.append("외국인합계_5일")
    display_cols = [c for c in display_cols if c in top.columns]

    top15 = top.head(15)
    top15_reset = top15.reset_index()
    top15_reset = top15_reset.rename(columns={top15_reset.columns[0]: "티커"})
    disp = top15_reset[["티커"] + display_cols].copy()
    if "거래대금" in disp.columns:
        disp["거래대금"] = (disp["거래대금"] / 1e8).round(1)
    if "기관합계_5일" in disp.columns:
        disp["기관합계_5일"] = (disp["기관합계_5일"] / 1e8).round(1)
    if "외국인합계_5일" in disp.columns:
        disp["외국인합계_5일"] = (disp["외국인합계_5일"] / 1e8).round(1)
    wl_tickers_before = {e["ticker"] for e in get_watchlist()}
    disp.insert(0, "⭐", disp["티커"].isin(wl_tickers_before))
    non_wl_cols = [c for c in disp.columns if c != "⭐"]
    edited = st.data_editor(
        disp,
        use_container_width=True,
        hide_index=True,
        column_config={
            "⭐": st.column_config.CheckboxColumn("⭐ 관심", width="small"),
            "등락률": st.column_config.NumberColumn(format="%.2f%%"),
            "거래대금": st.column_config.NumberColumn(format="%.1f억"),
            "기관합계_5일": st.column_config.NumberColumn(format="%.1f억"),
            "외국인합계_5일": st.column_config.NumberColumn(format="%.1f억"),
        },
        disabled=non_wl_cols,
    )
    changed = False
    for _, r in edited.iterrows():
        tkr = str(r["티커"])
        was_in = tkr in wl_tickers_before
        is_in = bool(r["⭐"])
        if is_in and not was_in:
            orig = top15_reset[top15_reset["티커"] == tkr]
            if not orig.empty:
                add_to_watchlist(
                    ticker=tkr,
                    name=str(orig.iloc[0].get("종목명", tkr)),
                    price=float(orig.iloc[0].get("종가", 0)),
                    sector=str(orig.iloc[0].get("업종", "")),
                    market=str(orig.iloc[0].get("시장", "")),
                )
                changed = True
        elif not is_in and was_in:
            remove_from_watchlist(tkr)
            changed = True
    if changed:
        st.rerun()

    # 개별 종목 상세 확인
    st.markdown("#### 🔍 개별 종목 돌파 분석")
    options = [""] + [
        f"{row.get('종목명', t)} ({t})"
        for t, row in top.iterrows()
        if pd.notna(row.get("종목명", ""))
    ]
    selected = st.selectbox("종목 선택 (돌파 분석)", options, key="type_c_breakout_detail")

    if selected:
        ticker = selected.split("(")[-1].rstrip(")")
        import datetime
        end_dt = datetime.datetime.strptime(date_str, "%Y%m%d")
        start_dt = end_dt - datetime.timedelta(days=400)
        with st.spinner("시세 데이터 로딩 중..."):
            ohlcv = get_stock_ohlcv_history(ticker, start_dt.strftime("%Y%m%d"), date_str)

        if not ohlcv.empty:
            high_info = check_52week_high(ticker, ohlcv)
            swing_info = check_swing_breakout(ohlcv, lookback=20)

            c1, c2 = st.columns(2)
            with c1:
                h_icon = "✅" if high_info["is_new_high"] else "❌"
                st.markdown(
                    f'<div style="background:#fff; border-radius:10px; padding:12px; '
                    f'border:1px solid #e2e8f0;">'
                    f'<div style="font-size:0.85em; color:#64748b;">52주 신고가</div>'
                    f'<div style="font-size:1.5em; font-weight:700;">{h_icon}'
                    f' {high_info["gap_pct"]:+.1f}%</div>'
                    f'<div style="font-size:0.75em; color:#94a3b8;">'
                    f'52주 고 {high_info["high_52w"]:,.0f} · 현재 {high_info["current_price"]:,.0f}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            with c2:
                s_icon = "✅" if swing_info["is_breakout"] else "❌"
                st.markdown(
                    f'<div style="background:#fff; border-radius:10px; padding:12px; '
                    f'border:1px solid #e2e8f0;">'
                    f'<div style="font-size:0.85em; color:#64748b;">20일 박스권 돌파</div>'
                    f'<div style="font-size:1.5em; font-weight:700;">{s_icon}'
                    f' {swing_info["direction"] or "박스내"}</div>'
                    f'<div style="font-size:0.75em; color:#94a3b8;">'
                    f'박스 {swing_info["box_low"]:,.0f}~{swing_info["box_high"]:,.0f}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.warning("시세 이력 데이터를 가져올 수 없습니다.")
