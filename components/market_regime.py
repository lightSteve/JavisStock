"""
📊 Panel 1: Market Regime Monitor (시장 국면 및 비중 스위치)
- 당일 코스닥/코스피 총 거래대금 트래커
- 주도 테마 맵 (상위 3개 테마)
- 권장 배팅 사이즈 시그널
"""

import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.fetcher import get_theme_list, get_theme_constituents


# ═══════════════════════════════════════════════════════════════════════════
# 메인 진입점
# ═══════════════════════════════════════════════════════════════════════════

def render_market_regime(daily_df: pd.DataFrame):
    """Panel 1: 시장 국면 모니터 렌더링."""
    st.markdown("## 📊 Market Regime Monitor")
    st.caption("시장 거래대금 · 주도 테마 · 권장 배팅 사이즈")

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    # ─── 1) 코스피 / 코스닥 거래대금 트래커 ───────────────────────
    _render_trading_value_tracker(daily_df)

    st.markdown("---")

    # ─── 2) 주도 테마 맵 ──────────────────────────────────────────
    _render_theme_map(daily_df)

    st.markdown("---")

    # ─── 3) 권장 배팅 사이즈 시그널 ──────────────────────────────
    _render_betting_signal(daily_df)


# ═══════════════════════════════════════════════════════════════════════════
# 1) 거래대금 트래커
# ═══════════════════════════════════════════════════════════════════════════

def _render_trading_value_tracker(daily_df: pd.DataFrame):
    """코스피/코스닥 총 거래대금 게이지 + 메트릭."""
    st.markdown("### 💰 시장 거래대금 트래커")

    has_market = "시장" in daily_df.columns

    if has_market:
        kospi_df = daily_df[daily_df["시장"] == "KOSPI"]
        kosdaq_df = daily_df[daily_df["시장"] == "KOSDAQ"]
    else:
        kospi_df = daily_df
        kosdaq_df = pd.DataFrame()

    kospi_tv = kospi_df["거래대금"].sum() / 1e12 if "거래대금" in kospi_df.columns else 0
    kosdaq_tv = kosdaq_df["거래대금"].sum() / 1e12 if not kosdaq_df.empty and "거래대금" in kosdaq_df.columns else 0
    total_tv = kospi_tv + kosdaq_tv

    # 메트릭 카드
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("🏛️ 코스피 거래대금", f"{kospi_tv:.1f}조")
    with c2:
        st.metric("📈 코스닥 거래대금", f"{kosdaq_tv:.1f}조")
    with c3:
        st.metric("📊 합산 거래대금", f"{total_tv:.1f}조")

    # 게이지 차트
    col1, col2 = st.columns(2)

    with col1:
        fig = _make_gauge(kospi_tv, "코스피", max_val=15, thresholds=[4, 7, 10])
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = _make_gauge(kosdaq_tv, "코스닥", max_val=15, thresholds=[3, 5, 8])
        st.plotly_chart(fig, use_container_width=True)

    # 시장 상태 판정
    regime = _classify_regime(total_tv)
    regime_colors = {
        "과열": "#dc2626",
        "활발": "#16a34a",
        "보통": "#f59e0b",
        "저조": "#6b7280",
    }
    color = regime_colors.get(regime, "#6b7280")
    st.markdown(
        f'<div style="text-align:center; padding:12px; background:{color}15; '
        f'border:2px solid {color}; border-radius:12px; margin:8px 0;">'
        f'<span style="font-size:1.3em; font-weight:700; color:{color};">'
        f'🔔 시장 컨디션: {regime} (합산 {total_tv:.1f}조)'
        f'</span></div>',
        unsafe_allow_html=True,
    )


def _make_gauge(value: float, title: str, max_val: float, thresholds: list) -> go.Figure:
    """거래대금 게이지 차트."""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        number={"suffix": "조", "font": {"size": 28}},
        title={"text": title, "font": {"size": 16}},
        gauge={
            "axis": {"range": [0, max_val], "tickwidth": 1, "ticksuffix": "조"},
            "bar": {"color": "#4f46e5"},
            "steps": [
                {"range": [0, thresholds[0]], "color": "#f1f5f9"},
                {"range": [thresholds[0], thresholds[1]], "color": "#fef3c7"},
                {"range": [thresholds[1], thresholds[2]], "color": "#bbf7d0"},
                {"range": [thresholds[2], max_val], "color": "#fecaca"},
            ],
            "threshold": {
                "line": {"color": "#dc2626", "width": 3},
                "thickness": 0.75,
                "value": thresholds[1],
            },
        },
    ))
    fig.update_layout(height=220, margin=dict(l=20, r=20, t=40, b=10))
    return fig


def _classify_regime(total_tv_조: float) -> str:
    """합산 거래대금 기반 시장 국면 분류."""
    if total_tv_조 >= 15:
        return "과열"
    elif total_tv_조 >= 8:
        return "활발"
    elif total_tv_조 >= 4:
        return "보통"
    return "저조"


# ═══════════════════════════════════════════════════════════════════════════
# 2) 주도 테마 맵
# ═══════════════════════════════════════════════════════════════════════════

def _render_theme_map(daily_df: pd.DataFrame):
    """당일 자금 집중 상위 3개 테마 시각화."""
    st.markdown("### 🗺️ 주도 테마 맵 (Top 3)")

    with st.spinner("테마 데이터 수집 중..."):
        theme_df = get_theme_list()

    if theme_df.empty:
        # 테마 스크래핑 실패 시, 업종 기반 대체 분석
        _render_sector_based_themes(daily_df)
        return

    # 등락률 상위 3개 테마
    top_themes = theme_df.nlargest(3, "등락률").reset_index(drop=True)

    cols = st.columns(3)
    rank_labels = ["🥇", "🥈", "🥉"]
    rank_colors = ["#f59e0b", "#9ca3af", "#b45309"]

    for i, (_, theme) in enumerate(top_themes.iterrows()):
        with cols[i]:
            change = theme["등락률"]
            color = "#dc2626" if change > 0 else "#2563eb" if change < 0 else "#6b7280"
            sign = "+" if change > 0 else ""

            st.markdown(
                f'<div style="background:#fff; border-radius:14px; padding:16px; '
                f'border:2px solid {rank_colors[i]}; text-align:center; '
                f'box-shadow:0 2px 8px rgba(0,0,0,0.06);">'
                f'<div style="font-size:1.5em;">{rank_labels[i]}</div>'
                f'<div style="font-size:1.05em; font-weight:700; color:#1e293b; margin:6px 0;">'
                f'{theme["테마명"]}</div>'
                f'<div style="font-size:1.4em; font-weight:800; color:{color};">'
                f'{sign}{change:.2f}%</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

            # 테마 구성 종목 (확장)
            theme_no = str(theme.get("테마번호", ""))
            if theme_no:
                with st.expander(f"📋 구성 종목 보기"):
                    const_df = get_theme_constituents(theme_no)
                    if not const_df.empty:
                        display_df = const_df.head(10)
                        for _, stock in display_df.iterrows():
                            chg = stock.get("등락률", 0)
                            chg_color = "#dc2626" if chg > 0 else "#2563eb" if chg < 0 else "#6b7280"
                            chg_sign = "+" if chg > 0 else ""
                            st.markdown(
                                f'<div style="display:flex; justify-content:space-between; '
                                f'padding:4px 0; border-bottom:1px solid #f1f5f9; font-size:0.85em;">'
                                f'<span style="color:#1e293b;">{stock["종목명"]}</span>'
                                f'<span style="color:{chg_color}; font-weight:600;">'
                                f'{chg_sign}{chg:.2f}%</span></div>',
                                unsafe_allow_html=True,
                            )
                    else:
                        st.caption("구성 종목 정보를 가져올 수 없습니다.")


def _render_sector_based_themes(daily_df: pd.DataFrame):
    """업종 데이터 기반 테마 대체 분석."""
    if "업종" not in daily_df.columns:
        st.info("테마/업종 데이터가 없습니다.")
        return

    sector_stats = daily_df.groupby("업종").agg(
        평균등락률=("등락률", "mean"),
        총거래대금=("거래대금", "sum"),
        종목수=("등락률", "count"),
    ).sort_values("총거래대금", ascending=False).head(5)

    if sector_stats.empty:
        st.info("업종별 데이터가 부족합니다.")
        return

    st.caption("⚠️ 테마 데이터 수집 불가 시, 업종별 자금 흐름으로 대체합니다.")

    cols = st.columns(min(3, len(sector_stats)))
    for i, (sector, row) in enumerate(sector_stats.head(3).iterrows()):
        with cols[i]:
            chg = row["평균등락률"]
            color = "#dc2626" if chg > 0 else "#2563eb" if chg < 0 else "#6b7280"
            tv_억 = row["총거래대금"] / 1e8
            st.markdown(
                f'<div style="background:#fff; border-radius:14px; padding:16px; '
                f'border:1px solid #e2e8f0; text-align:center;">'
                f'<div style="font-size:1.05em; font-weight:700; color:#1e293b;">{sector}</div>'
                f'<div style="font-size:1.2em; font-weight:700; color:{color}; margin:6px 0;">'
                f'{"+" if chg > 0 else ""}{chg:.2f}%</div>'
                f'<div style="font-size:0.78em; color:#64748b;">'
                f'거래대금 {tv_억:,.0f}억 · {int(row["종목수"])}종목</div>'
                f'</div>',
                unsafe_allow_html=True,
            )


# ═══════════════════════════════════════════════════════════════════════════
# 3) 권장 배팅 사이즈
# ═══════════════════════════════════════════════════════════════════════════

def _render_betting_signal(daily_df: pd.DataFrame):
    """시장 컨디션 기반 권장 배팅 사이즈 시그널."""
    st.markdown("### 🎯 권장 배팅 사이즈")

    total_tv = daily_df["거래대금"].sum() / 1e12 if "거래대금" in daily_df.columns else 0

    # 상승/하락
    up_ratio = (daily_df["등락률"] > 0).mean() * 100 if "등락률" in daily_df.columns else 50
    avg_change = daily_df["등락률"].mean() if "등락률" in daily_df.columns else 0

    # 변동성 (등락률 표준편차)
    volatility = daily_df["등락률"].std() if "등락률" in daily_df.columns else 0

    # 배팅 사이즈 계산 (0~100)
    tv_score = min(total_tv / 15 * 100, 100)  # 15조 이상이면 100
    breadth_score = min(up_ratio / 60 * 100, 100)  # 상승 비율
    vol_score = min(volatility / 3 * 100, 100)  # 변동성

    # 종합 배팅 사이즈 (거래대금 50% + 시장폭 30% + 변동성 20%)
    betting_size = int(tv_score * 0.5 + breadth_score * 0.3 + vol_score * 0.2)
    betting_size = max(10, min(100, betting_size))

    # 배팅 등급
    if betting_size >= 80:
        grade = "🔴 풀배팅"
        grade_color = "#dc2626"
        desc = "시장 거래대금·변동성 모두 활발합니다. 비중을 최대로 가져갑니다."
    elif betting_size >= 60:
        grade = "🟠 공격적"
        grade_color = "#ea580c"
        desc = "시장 활기가 양호합니다. 주요 테마 위주로 적극 매매합니다."
    elif betting_size >= 40:
        grade = "🟡 중립"
        grade_color = "#f59e0b"
        desc = "시장 상황이 보통입니다. 핵심 종목 위주로 선별 대응합니다."
    elif betting_size >= 20:
        grade = "🟢 보수적"
        grade_color = "#16a34a"
        desc = "시장 거래대금이 낮습니다. 비중을 축소하고 관망합니다."
    else:
        grade = "⚪ 관망"
        grade_color = "#6b7280"
        desc = "시장이 극도로 위축되어 있습니다. 현금 비중을 높입니다."

    col1, col2 = st.columns([1, 2])

    with col1:
        # 배팅 사이즈 표시
        st.markdown(
            f'<div style="text-align:center; background:#fff; border-radius:16px; '
            f'padding:24px; border:2px solid {grade_color};">'
            f'<div style="font-size:0.85em; color:#64748b;">권장 배팅 사이즈</div>'
            f'<div style="font-size:3em; font-weight:800; color:{grade_color};">{betting_size}%</div>'
            f'<div style="font-size:1.1em; font-weight:700; color:{grade_color};">{grade}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(f"**📋 판단 근거:**")
        st.markdown(f"> {desc}")

        # 세부 지표
        st.markdown(
            f'<div style="display:flex; gap:12px; flex-wrap:wrap; margin-top:12px;">'
            f'<div style="flex:1; min-width:100px; background:#f8fafc; border-radius:8px; padding:10px; text-align:center;">'
            f'<div style="font-size:0.72em; color:#64748b;">거래대금 점수</div>'
            f'<div style="font-size:1.2em; font-weight:700; color:#4f46e5;">{tv_score:.0f}</div></div>'
            f'<div style="flex:1; min-width:100px; background:#f8fafc; border-radius:8px; padding:10px; text-align:center;">'
            f'<div style="font-size:0.72em; color:#64748b;">시장폭 점수</div>'
            f'<div style="font-size:1.2em; font-weight:700; color:#4f46e5;">{breadth_score:.0f}</div></div>'
            f'<div style="flex:1; min-width:100px; background:#f8fafc; border-radius:8px; padding:10px; text-align:center;">'
            f'<div style="font-size:0.72em; color:#64748b;">변동성 점수</div>'
            f'<div style="font-size:1.2em; font-weight:700; color:#4f46e5;">{vol_score:.0f}</div></div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # 슬라이더로 사용자 조정 가능
        user_size = st.slider(
            "배팅 사이즈 수동 조절",
            min_value=0, max_value=100, value=betting_size,
            help="시스템 권장값을 기준으로 본인 판단에 따라 조정하세요.",
            key="betting_slider",
        )
        if user_size != betting_size:
            st.info(f"📌 수동 조절 배팅 사이즈: **{user_size}%** (시스템 권장: {betting_size}%)")
