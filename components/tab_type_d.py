"""
📊 Type D: 섹터별 급락 → 회복 패턴 (전 섹터 대응)
- 섹터 흐름 현황 (급락/하락/회복/상승)
- 섹터별 급락 종목 스캐너
- 급락 후 회복 종목 발굴
"""

import datetime
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from data.fetcher import (
    get_stock_news_list,
    get_stock_ohlcv_history,
    get_investor_trend_individual,
)
from logic_patterns import (
    analyze_all_sectors,
    detect_sector_crash_stocks,
    detect_sector_recovering_stocks,
    calc_recovery_stats,
)


def render_tab_type_d(daily_df: pd.DataFrame, date_str: str):
    """Type D 탭 렌더링: 섹터별 급락 → 회복 패턴."""
    st.markdown("## 📊 Type D: 섹터별 급락 → 회복 패턴")
    st.caption("전체 섹터 흐름 · 급락 스캔 · 회복 종목 발굴")

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    sector_summary = analyze_all_sectors(daily_df)
    if sector_summary.empty:
        st.warning("섹터(업종) 데이터가 없습니다.")
        return

    tab_overview, tab_scan, tab_recover = st.tabs([
        "🗺️ 섹터 현황", "🔍 급락 스캐너", "📈 회복 종목"])

    with tab_overview:
        _render_sector_overview(sector_summary)

    with tab_scan:
        _render_sector_scanner(daily_df, sector_summary, date_str)

    with tab_recover:
        _render_sector_recovery(daily_df, sector_summary, date_str)


# ─────────────────────────────────────────────────────────────────────
def _render_sector_overview(sector_summary: pd.DataFrame):
    """전체 섹터 흐름 현황판."""
    st.markdown("### 🗺️ 섹터별 현재 흐름")

    # 국면별 카운트
    phase_counts = sector_summary["국면"].value_counts()
    phase_order = ["급락", "하락", "회복", "상승"]
    phase_colors = {"급락": "#dc2626", "하락": "#ea580c", "회복": "#f59e0b", "상승": "#16a34a"}
    phase_icons = {"급락": "🔴", "하락": "🟠", "회복": "🟡", "상승": "🟢"}

    cols = st.columns(4)
    for i, phase in enumerate(phase_order):
        cnt = phase_counts.get(phase, 0)
        with cols[i]:
            st.markdown(
                f'<div style="background:{phase_colors[phase]}15; border:2px solid {phase_colors[phase]}; '
                f'border-radius:12px; padding:10px; text-align:center;">'
                f'<div style="font-size:1.6em;">{phase_icons[phase]}</div>'
                f'<div style="font-size:1.4em; font-weight:800; color:{phase_colors[phase]};">{cnt}</div>'
                f'<div style="font-size:0.8em; color:#64748b;">{phase}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.markdown("")

    # 섹터 카드 그리드 (국면별 그룹)
    for phase in phase_order:
        phase_df = sector_summary[sector_summary["국면"] == phase]
        if phase_df.empty:
            continue

        color = phase_colors[phase]
        icon = phase_icons[phase]
        st.markdown(
            f'<div style="font-size:1em; font-weight:700; color:{color}; '
            f'margin:12px 0 6px 0;">{icon} {phase} 섹터 ({len(phase_df)}개)</div>',
            unsafe_allow_html=True,
        )

        # 3열 그리드
        grid_cols = st.columns(3)
        for idx, (_, row) in enumerate(phase_df.iterrows()):
            with grid_cols[idx % 3]:
                avg_chg = row["평균등락률"]
                chg_color = "#dc2626" if avg_chg > 0 else "#2563eb"
                crash_cnt = int(row["급락종목수"])
                crash_text = f" · 급락 {crash_cnt}개" if crash_cnt > 0 else ""
                st.markdown(
                    f'<div style="background:#fff; border-radius:10px; padding:8px 12px; '
                    f'border-left:4px solid {color}; margin-bottom:6px; '
                    f'box-shadow:0 1px 3px rgba(0,0,0,0.05);">'
                    f'<div style="font-weight:700; font-size:0.88em;">{row["섹터"]}</div>'
                    f'<div style="font-size:1.05em; font-weight:800; color:{chg_color};">'
                    f'{avg_chg:+.2f}%</div>'
                    f'<div style="font-size:0.72em; color:#94a3b8;">'
                    f'{row["종목수"]}종목 · 상승 {row["상승비율"]:.0f}%'
                    f'{crash_text}'
                    f'</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )


# ─────────────────────────────────────────────────────────────────────
def _render_sector_scanner(daily_df: pd.DataFrame, sector_summary: pd.DataFrame, date_str: str):
    """섹터별 급락 종목 스캐너."""
    st.markdown("### 🔍 섹터별 급락 스캐너")

    # 급락/하락 섹터 우선 표시
    problem_sectors = sector_summary[sector_summary["국면"].isin(["급락", "하락"])]
    all_sectors = sector_summary["섹터"].tolist()

    threshold = st.slider("급락 기준 (%)", -20.0, -3.0, -5.0, 0.5, key="sector_crash_threshold")

    # 섹터 선택
    if not problem_sectors.empty:
        default_options = problem_sectors["섹터"].tolist()
        st.info(f"현재 급락/하락 국면 섹터: {', '.join(default_options)}")
    else:
        default_options = []

    selected_sectors = st.multiselect(
        "분석할 섹터 선택",
        options=all_sectors,
        default=default_options[:5],
        key="sector_scan_select",
    )

    if not selected_sectors:
        st.info("섹터를 선택하면 해당 섹터의 급락 종목을 보여줍니다.")
        return

    for sector in selected_sectors:
        crash = detect_sector_crash_stocks(daily_df, sector, threshold=threshold)
        sector_info = sector_summary[sector_summary["섹터"] == sector]
        phase = sector_info["국면"].values[0] if not sector_info.empty else "?"
        phase_color_map = {"급락": "#dc2626", "하락": "#ea580c", "회복": "#f59e0b", "상승": "#16a34a"}
        p_color = phase_color_map.get(phase, "#64748b")

        st.markdown(
            f'<div style="font-size:0.95em; font-weight:700; margin:14px 0 6px 0; '
            f'padding:6px 12px; background:{p_color}15; border-left:4px solid {p_color}; '
            f'border-radius:6px;">'
            f'{sector} '
            f'<span style="font-size:0.78em; color:{p_color}; font-weight:600;">({phase})</span>'
            f' — 급락 {len(crash)}종목'
            f'</div>',
            unsafe_allow_html=True,
        )

        if crash.empty:
            st.caption(f"  ✅ {sector} 섹터에 급락 종목이 없습니다.")
            continue

        for ticker, row in crash.head(10).iterrows():
            name = row.get("종목명", ticker)
            change = row.get("등락률", 0)
            price = row.get("종가", 0)
            tv = row.get("거래대금", 0) / 1e8

            if change <= -20:
                sev, sev_color = "심각", "#991b1b"
            elif change <= -10:
                sev, sev_color = "경계", "#c2410c"
            else:
                sev, sev_color = "주의", "#a16207"

            st.markdown(
                f'<div style="background:#fff; border-radius:8px; padding:8px 12px; '
                f'border-left:3px solid {sev_color}; margin-bottom:4px;">'
                f'<div style="display:flex; justify-content:space-between; align-items:center;">'
                f'<span style="font-weight:700; font-size:0.88em;">{name}'
                f'<span style="color:#94a3b8; font-size:0.78em; margin-left:4px;">{ticker}</span></span>'
                f'<span style="color:{sev_color}; font-weight:700; font-size:0.88em;">{change:.1f}% {sev}</span>'
                f'</div>'
                f'<div style="font-size:0.72em; color:#64748b;">'
                f'{price:,.0f}원 · 거래대금 {tv:,.0f}억</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

            with st.expander(f"📰 {name} 뉴스"):
                news = get_stock_news_list(ticker)
                if news:
                    for item in news[:5]:
                        st.markdown(f"- {item.get('title', '')}")
                else:
                    st.caption("최근 관련 뉴스 없음")


# ─────────────────────────────────────────────────────────────────────
def _render_sector_recovery(daily_df: pd.DataFrame, sector_summary: pd.DataFrame, date_str: str):
    """섹터별 급락 후 회복 종목 발굴."""
    st.markdown("### 📈 섹터별 회복 종목 발굴")
    st.caption("급락/하락 국면 섹터에서 반등 신호를 보이는 종목")

    # 급락·하락·회복 국면 섹터를 대상으로 분석
    target_phases = ["급락", "하락", "회복"]
    target_sectors = sector_summary[sector_summary["국면"].isin(target_phases)]

    if target_sectors.empty:
        st.success("✅ 현재 급락/하락 국면의 섹터가 없습니다. 모든 섹터가 상승 흐름입니다!")
        return

    phase_color_map = {"급락": "#dc2626", "하락": "#ea580c", "회복": "#f59e0b", "상승": "#16a34a"}

    for _, sec_row in target_sectors.iterrows():
        sector = sec_row["섹터"]
        phase = sec_row["국면"]
        p_color = phase_color_map.get(phase, "#64748b")

        recovering = detect_sector_recovering_stocks(daily_df, sector)

        st.markdown(
            f'<div style="font-size:0.95em; font-weight:700; margin:14px 0 6px 0; '
            f'padding:6px 12px; background:{p_color}15; border-left:4px solid {p_color}; '
            f'border-radius:6px;">'
            f'{sector} '
            f'<span style="font-size:0.78em; color:{p_color};">({phase} → 회복 후보 {len(recovering)}종목)</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

        if recovering.empty:
            st.caption(f"  {sector}: 아직 반등 신호 종목이 없습니다.")
            continue

        for ticker, row in recovering.head(8).iterrows():
            name = row.get("종목명", ticker)
            change = row.get("등락률", 0)
            price = row.get("종가", 0)
            tv = row.get("거래대금", 0) / 1e8

            st.markdown(
                f'<div style="background:#fff; border-radius:8px; padding:8px 12px; '
                f'border-left:3px solid #16a34a; margin-bottom:4px;">'
                f'<div style="display:flex; justify-content:space-between; align-items:center;">'
                f'<span style="font-weight:700; font-size:0.88em;">🟢 {name}'
                f'<span style="color:#94a3b8; font-size:0.78em; margin-left:4px;">{ticker}</span></span>'
                f'<span style="color:#16a34a; font-weight:700; font-size:0.88em;">+{change:.1f}%</span>'
                f'</div>'
                f'<div style="font-size:0.72em; color:#64748b;">'
                f'{price:,.0f}원 · 거래대금 {tv:,.0f}억</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    # 개별 종목 상세 분석 (선택형)
    st.markdown("---")
    st.markdown("#### 🔬 개별 종목 회복 분석")

    all_sectors_list = target_sectors["섹터"].tolist()
    sel_sector = st.selectbox("섹터 선택", [""] + all_sectors_list, key="recovery_sector_sel")

    if not sel_sector:
        st.info("섹터를 선택하면 회복 중인 종목의 상세 분석을 할 수 있습니다.")
        return

    candidates = detect_sector_recovering_stocks(daily_df, sel_sector)
    if candidates.empty:
        st.info(f"{sel_sector} 섹터에 회복 종목이 없습니다.")
        return

    options = [""] + [
        f"{row.get('종목명', t)} ({t})" for t, row in candidates.iterrows()
    ]
    selected = st.selectbox("종목 선택 (회복 분석)", options, key="type_d_recovery_stock")

    if not selected:
        return

    ticker = selected.split("(")[-1].rstrip(")")

    end_dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    start_dt = end_dt - datetime.timedelta(days=120)

    with st.spinner("시세 데이터 로딩 중..."):
        ohlcv = get_stock_ohlcv_history(ticker, start_dt.strftime("%Y%m%d"), date_str)

    if ohlcv.empty:
        st.warning("시세 데이터를 가져올 수 없습니다.")
        return

    stats = calc_recovery_stats(ohlcv)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("급락폭", f"{stats['crash_pct']:.1f}%")
    with c2:
        st.metric("경과일", f"{stats['days_since']}일")
    with c3:
        rec_color = "normal" if stats["recovery_pct"] > 0 else "inverse"
        st.metric("회복률", f"{stats['recovery_pct']:+.1f}%", delta_color=rec_color)

    # 캔들 차트
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

    # 수급
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
