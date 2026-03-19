"""
📰 Type B: 뉴스/속보 스파이크 매매
- 뉴스 임팩트 스코어 기반 종목 발굴
- 거래량 급증 + 양봉 종목 필터
- 뉴스 키워드 모니터링
"""

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from data.fetcher import get_stock_news_list, detect_volume_spike_stocks
from logic_patterns import calc_news_impact_score, detect_news_spike_candidates
from components.watchlist import get_watchlist, add_to_watchlist, remove_from_watchlist


def render_tab_type_b(daily_df: pd.DataFrame, date_str: str):
    """Type B 탭 렌더링: 뉴스/속보 스파이크 매매."""
    st.markdown("## 📰 Type B: 뉴스 · 속보 스파이크")
    st.caption("거래량 급증 + 뉴스 임팩트 기반 단타 진입 타점")

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    # ─── 1) 스파이크 후보 리스트 ───────────────────────────────
    _render_spike_candidates(daily_df)

    st.markdown("---")

    # ─── 2) 뉴스 검색 & 임팩트 분석 ──────────────────────────
    _render_news_scanner(daily_df, date_str)


# ─────────────────────────────────────────────────────────────────────
def _render_spike_candidates(daily_df: pd.DataFrame):
    """거래량 급증 + 양봉 종목."""
    st.markdown("### ⚡ 스파이크 후보 종목")

    candidates = detect_news_spike_candidates(daily_df)
    if candidates.empty:
        st.info("스파이크 조건을 충족하는 종목이 없습니다.")
        return

    top = candidates.head(15)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("📊 후보 종목 수", f"{len(candidates)}개")
    with c2:
        avg_score = top["spike_score"].mean()
        st.metric("평균 스파이크 점수", f"{avg_score:.1f}")

    # 차트: 스파이크 점수 바
    fig = go.Figure(go.Bar(
        y=[f"{row.get('종목명', t)} ({t})" for t, row in top.iterrows()][::-1],
        x=top["spike_score"].values[::-1],
        orientation="h",
        marker_color=["#dc2626" if s >= 70 else "#f59e0b" if s >= 40 else "#4f46e5"
                       for s in top["spike_score"].values[::-1]],
        text=[f"{s:.0f}" for s in top["spike_score"].values[::-1]],
        textposition="outside",
    ))
    fig.update_layout(
        title="스파이크 점수 TOP 15",
        height=max(300, len(top) * 30),
        margin=dict(l=10, r=60, t=40, b=10),
        template="plotly_white",
    )
    st.plotly_chart(fig, use_container_width=True)

    # 상세 테이블
    with st.expander("📋 상세 데이터 (관심종목 추가 가능)"):
        top_reset = top.reset_index()
        top_reset = top_reset.rename(columns={top_reset.columns[0]: "티커"})
        display_cols = ["티커", "종목명", "등락률", "거래대금", "spike_score"]
        display_cols = [c for c in display_cols if c in top_reset.columns]
        df_disp = top_reset[display_cols].copy()
        if "거래대금" in df_disp.columns:
            df_disp["거래대금"] = (df_disp["거래대금"] / 1e8).round(1)
        wl_tickers_before = {e["ticker"] for e in get_watchlist()}
        df_disp.insert(0, "⭐", df_disp["티커"].isin(wl_tickers_before))
        non_wl_cols = [c for c in df_disp.columns if c != "⭐"]
        edited = st.data_editor(
            df_disp,
            use_container_width=True,
            hide_index=True,
            column_config={
                "⭐": st.column_config.CheckboxColumn("⭐ 관심", width="small"),
                "등락률": st.column_config.NumberColumn(format="%.2f%%"),
                "거래대금": st.column_config.NumberColumn(format="%.1f억"),
                "spike_score": st.column_config.NumberColumn(format="%.0f"),
            },
            disabled=non_wl_cols,
        )
        changed = False
        for _, r in edited.iterrows():
            tkr = str(r["티커"])
            was_in = tkr in wl_tickers_before
            is_in = bool(r["⭐"])
            if is_in and not was_in:
                orig = top_reset[top_reset["티커"] == tkr]
                if not orig.empty:
                    add_to_watchlist(
                        ticker=tkr,
                        name=str(orig.iloc[0].get("종목명", tkr)),
                        price=float(orig.iloc[0].get("종가", 0)),
                        sector=str(orig.iloc[0].get("업종", "")),
                        market=str(orig.iloc[0].get("시장", "")),
                        source="📰 B:뉴스스파이크",
                    )
                    changed = True
            elif not is_in and was_in:
                remove_from_watchlist(tkr)
                changed = True
        if changed:
            st.rerun()


# ─────────────────────────────────────────────────────────────────────
def _render_news_scanner(daily_df: pd.DataFrame, date_str: str):
    """개별 종목 뉴스 검색 & 임팩트 점수."""
    st.markdown("### 📰 종목 뉴스 임팩트 분석")

    # 종목 선택
    options = [""] + [
        f"{row.get('종목명', t)} ({t})"
        for t, row in daily_df.head(200).iterrows()
        if pd.notna(row.get("종목명", ""))
    ]
    selected = st.selectbox("종목 선택", options, key="type_b_news_stock")

    if not selected:
        st.info("종목을 선택하면 뉴스 임팩트 점수를 분석합니다.")
        return

    ticker = selected.split("(")[-1].rstrip(")")

    with st.spinner("뉴스 수집 중..."):
        news_list = get_stock_news_list(ticker)

    if not news_list:
        st.info(f"{selected}: 최근 뉴스가 없습니다.")
        return

    impact = calc_news_impact_score(news_list)

    # 임팩트 게이지
    if impact >= 70:
        grade, color = "🔴 HIGH", "#dc2626"
    elif impact >= 40:
        grade, color = "🟡 MED", "#f59e0b"
    else:
        grade, color = "🟢 LOW", "#16a34a"

    st.markdown(
        f'<div style="text-align:center; padding:12px; background:{color}15; '
        f'border:2px solid {color}; border-radius:12px;">'
        f'<span style="font-size:0.85em; color:#64748b;">뉴스 임팩트 점수</span>'
        f'<div style="font-size:2.5em; font-weight:800; color:{color};">{impact:.0f}</div>'
        f'<div style="font-size:1em; font-weight:700; color:{color};">{grade}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # 뉴스 리스트
    st.markdown("#### 📋 최근 뉴스")
    for item in news_list[:10]:
        title = item.get("title", "")
        date = item.get("date", "")
        st.markdown(
            f'<div style="padding:6px 0; border-bottom:1px solid #f1f5f9; font-size:0.88em;">'
            f'<span style="color:#1e293b;">{title}</span>'
            f'<span style="color:#94a3b8; margin-left:8px; font-size:0.8em;">{date}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
