"""
🔀 Panel 2: Pair Trading & Theme Leader Board
(유형 A 공략: 짝꿍 매매)
- 테마별 서열 추적기 (1등주·2등주 페어링)
- 1등주 상한가 잠김 알림 & 후발주 매수 타점
- 잔량 및 매도벽 모니터링 (상한가 종목 체결 강도)
"""

import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go

from data.fetcher import (
    get_theme_list,
    get_theme_constituents,
    detect_limit_up_stocks,
)


# ═══════════════════════════════════════════════════════════════════════════
# 메인 진입점
# ═══════════════════════════════════════════════════════════════════════════

def render_pair_trading(daily_df: pd.DataFrame, date_str: str):
    """Panel 2: 짝꿍 매매 & 테마 리더보드 렌더링."""
    st.markdown("## 🔀 Pair Trading & Theme Leader Board")
    st.caption("테마별 서열 추적 · 상한가 잠김 알림 · 후발주 매수 타점")

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    # ─── 1) 상한가 / 급등 대장주 현황 ────────────────────────────
    _render_limit_up_dashboard(daily_df)

    st.markdown("---")

    # ─── 2) 테마별 서열 추적기 ──────────────────────────────────
    _render_theme_leader_board(daily_df)

    st.markdown("---")

    # ─── 3) 짝꿍 매매 시그널 (1등주 잠금 → 2등주 타점) ──────────
    _render_pair_signals(daily_df)


# ═══════════════════════════════════════════════════════════════════════════
# 1) 상한가 / 급등 대장주 현황
# ═══════════════════════════════════════════════════════════════════════════

def _render_limit_up_dashboard(daily_df: pd.DataFrame):
    """상한가 및 급등 종목 대시보드."""
    st.markdown("### 🚀 상한가 · 급등 대장주 현황")

    limit_up = detect_limit_up_stocks(daily_df, threshold=29.0)
    strong_up = daily_df[
        (daily_df["등락률"] >= 15) & (daily_df["등락률"] < 29)
    ].sort_values("등락률", ascending=False) if "등락률" in daily_df.columns else pd.DataFrame()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("🔒 상한가 종목", f"{len(limit_up)}개")
    with c2:
        st.metric("🔥 급등 (15%+)", f"{len(strong_up)}개")
    with c3:
        total_hot = len(limit_up) + len(strong_up)
        st.metric("📊 핫 종목 합계", f"{total_hot}개")

    if not limit_up.empty:
        st.markdown("#### 🔒 상한가 종목 리스트")
        _render_hot_stock_cards(limit_up.head(10), badge="상한가", badge_color="#dc2626")

    if not strong_up.empty:
        with st.expander(f"🔥 급등 종목 ({len(strong_up)}개)", expanded=len(limit_up) == 0):
            _render_hot_stock_cards(strong_up.head(10), badge="급등", badge_color="#ea580c")


def _render_hot_stock_cards(df: pd.DataFrame, badge: str, badge_color: str):
    """급등/상한가 종목 카드 그리드."""
    cols = st.columns(min(5, max(1, len(df))))
    for i, (ticker, row) in enumerate(df.iterrows()):
        with cols[i % len(cols)]:
            name = row.get("종목명", ticker)
            change = row.get("등락률", 0)
            price = row.get("종가", 0)
            tv = row.get("거래대금", 0) / 1e8  # 억 단위

            sector = row.get("업종", "")
            inst = row.get("기관합계_5일", 0) / 1e8 if "기관합계_5일" in row.index else 0
            frgn = row.get("외국인합계_5일", 0) / 1e8 if "외국인합계_5일" in row.index else 0

            # 수급 색상
            supply_color = "#16a34a" if (inst > 0 and frgn > 0) else "#f59e0b" if (inst > 0 or frgn > 0) else "#6b7280"

            st.markdown(
                f'<div style="background:#fff; border-radius:12px; padding:12px; '
                f'border-left:4px solid {badge_color}; margin-bottom:8px; '
                f'box-shadow:0 2px 6px rgba(0,0,0,0.05);">'
                f'<div style="display:flex; justify-content:space-between; align-items:center;">'
                f'<span style="background:{badge_color}; color:#fff; padding:1px 8px; '
                f'border-radius:6px; font-size:0.65em; font-weight:700;">{badge}</span>'
                f'<span style="font-size:0.65em; color:#94a3b8;">{ticker}</span>'
                f'</div>'
                f'<div style="font-weight:700; font-size:0.95em; color:#1e293b; margin:4px 0;">{name}</div>'
                f'<div style="font-size:1.1em; font-weight:800; color:#dc2626;">+{change:.1f}%</div>'
                f'<div style="font-size:0.75em; color:#64748b; margin-top:4px;">'
                f'{price:,.0f}원 · 거래대금 {tv:,.0f}억</div>'
                f'<div style="font-size:0.7em; margin-top:4px; color:{supply_color};">'
                f'🏛️ {inst:+,.0f}억 · 🌍 {frgn:+,.0f}억</div>'
                f'{"<div style=font-size:0.65em;color:#94a3b8;margin-top:2px;>" + sector + "</div>" if sector else ""}'
                f'</div>',
                unsafe_allow_html=True,
            )


# ═══════════════════════════════════════════════════════════════════════════
# 2) 테마별 서열 추적기
# ═══════════════════════════════════════════════════════════════════════════

def _render_theme_leader_board(daily_df: pd.DataFrame):
    """테마별 1등주/2등주 서열 추적."""
    st.markdown("### 📋 테마별 서열 추적기")

    from data.scheduler import get_cached_theme_list
    with st.spinner("테마 서열 분석 중..."):
        theme_df = get_cached_theme_list()
        if theme_df is None:
            theme_df = get_theme_list()
        else:
            theme_df = theme_df.copy()

    if theme_df.empty:
        # 업종 기반 대체 분석
        _render_sector_leader_board(daily_df)
        return

    # 등락률 기준 상위 테마
    top_themes = theme_df.nlargest(6, "등락률")

    if top_themes.empty:
        st.info("활발한 테마가 없습니다.")
        return

    for _, theme in top_themes.iterrows():
        theme_name = theme["테마명"]
        theme_no = str(theme.get("테마번호", ""))
        theme_change = theme["등락률"]

        if not theme_no:
            continue

        const_df = get_theme_constituents(theme_no)
        if const_df.empty:
            continue

        # 등락률 기준 1등주, 2등주 찾기
        const_sorted = const_df.sort_values("등락률", ascending=False)
        leader = const_sorted.iloc[0] if len(const_sorted) >= 1 else None
        runner = const_sorted.iloc[1] if len(const_sorted) >= 2 else None

        if leader is None:
            continue

        leader_locked = leader["등락률"] >= 29.0  # 상한가 잠김 여부

        with st.expander(
            f"{'🔒' if leader_locked else '📊'} {theme_name} (등락률 {theme_change:+.2f}%)",
            expanded=leader_locked,
        ):
            col1, col2 = st.columns(2)

            # 1등주 카드
            with col1:
                lock_badge = "🔒 상한가 잠김" if leader_locked else "👑 1등주"
                lock_color = "#dc2626" if leader_locked else "#f59e0b"
                st.markdown(
                    f'<div style="background:linear-gradient(135deg, {lock_color}08, {lock_color}15); '
                    f'border-radius:12px; padding:14px; border:1px solid {lock_color}40;">'
                    f'<div style="font-size:0.72em; color:{lock_color}; font-weight:700;">{lock_badge}</div>'
                    f'<div style="font-size:1.1em; font-weight:700; color:#1e293b; margin:4px 0;">'
                    f'{leader["종목명"]}</div>'
                    f'<div style="font-size:0.8em; color:#64748b;">{leader["티커"]}</div>'
                    f'<div style="font-size:1.3em; font-weight:800; color:#dc2626; margin-top:6px;">'
                    f'+{leader["등락률"]:.1f}%</div>'
                    f'<div style="font-size:0.78em; color:#64748b;">현재가 {leader["현재가"]:,}원</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

            # 2등주 카드 (후발주)
            with col2:
                if runner is not None:
                    # 1등주 잠기면 2등주에 매수 타점 시그널
                    signal = ""
                    if leader_locked and runner["등락률"] < 15:
                        signal = (
                            '<div style="background:#fef3c7; color:#92400e; padding:4px 8px; '
                            'border-radius:6px; font-size:0.7em; font-weight:700; margin-bottom:6px;">'
                            '⚡ 매수 타점 — 1등주 잠김 후, 후발주 진입 구간</div>'
                        )
                    elif leader_locked and runner["등락률"] >= 15:
                        signal = (
                            '<div style="background:#fee2e2; color:#991b1b; padding:4px 8px; '
                            'border-radius:6px; font-size:0.7em; font-weight:700; margin-bottom:6px;">'
                            '⚠️ 주의 — 후발주도 이미 크게 상승</div>'
                        )

                    runner_change = runner["등락률"]
                    r_color = "#dc2626" if runner_change > 0 else "#2563eb" if runner_change < 0 else "#6b7280"
                    st.markdown(
                        f'<div style="background:#f0f9ff; border-radius:12px; padding:14px; '
                        f'border:1px solid #bae6fd;">'
                        f'{signal}'
                        f'<div style="font-size:0.72em; color:#2563eb; font-weight:700;">🎯 2등주 (후발주)</div>'
                        f'<div style="font-size:1.1em; font-weight:700; color:#1e293b; margin:4px 0;">'
                        f'{runner["종목명"]}</div>'
                        f'<div style="font-size:0.8em; color:#64748b;">{runner["티커"]}</div>'
                        f'<div style="font-size:1.3em; font-weight:800; color:{r_color}; margin-top:6px;">'
                        f'{"+" if runner_change > 0 else ""}{runner_change:.1f}%</div>'
                        f'<div style="font-size:0.78em; color:#64748b;">현재가 {runner["현재가"]:,}원</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption("2등주 데이터 없음")


def _render_sector_leader_board(daily_df: pd.DataFrame):
    """업종 기반 대체 서열 분석."""
    if "업종" not in daily_df.columns:
        st.info("업종 데이터가 없습니다.")
        return

    st.caption("⚠️ 테마 데이터 수집 불가 시, 업종별 대장주 서열로 대체합니다.")

    # 업종별 상위 종목
    sector_groups = daily_df.groupby("업종")
    hot_sectors = daily_df.groupby("업종")["등락률"].mean().nlargest(5)

    for sector_name, avg_change in hot_sectors.items():
        sector_stocks = sector_groups.get_group(sector_name).sort_values("등락률", ascending=False)
        leader = sector_stocks.iloc[0] if len(sector_stocks) >= 1 else None
        runner = sector_stocks.iloc[1] if len(sector_stocks) >= 2 else None

        if leader is None:
            continue

        with st.expander(f"📊 {sector_name} (평균 {avg_change:+.2f}%)"):
            col1, col2 = st.columns(2)
            with col1:
                l_chg = leader.get("등락률", 0)
                l_name = leader.get("종목명", leader.name)
                st.markdown(
                    f'<div style="background:#fffbeb; border-radius:10px; padding:12px; '
                    f'border:1px solid #fde68a;">'
                    f'<div style="font-size:0.7em; color:#92400e; font-weight:700;">👑 1등주</div>'
                    f'<div style="font-weight:700; color:#1e293b;">{l_name}</div>'
                    f'<div style="font-weight:800; color:#dc2626;">+{l_chg:.1f}%</div></div>',
                    unsafe_allow_html=True,
                )
            with col2:
                if runner is not None:
                    r_chg = runner.get("등락률", 0)
                    r_name = runner.get("종목명", runner.name)
                    r_color = "#dc2626" if r_chg > 0 else "#2563eb"
                    st.markdown(
                        f'<div style="background:#f0f9ff; border-radius:10px; padding:12px; '
                        f'border:1px solid #bae6fd;">'
                        f'<div style="font-size:0.7em; color:#2563eb; font-weight:700;">🎯 2등주</div>'
                        f'<div style="font-weight:700; color:#1e293b;">{r_name}</div>'
                        f'<div style="font-weight:800; color:{r_color};">{"+" if r_chg > 0 else ""}{r_chg:.1f}%</div></div>',
                        unsafe_allow_html=True,
                    )


# ═══════════════════════════════════════════════════════════════════════════
# 3) 짝꿍 매매 시그널
# ═══════════════════════════════════════════════════════════════════════════

def _render_pair_signals(daily_df: pd.DataFrame):
    """상한가 잠김 + 후발주 매수 타점 시그널 종합."""
    st.markdown("### ⚡ 짝꿍 매매 시그널")

    limit_up = detect_limit_up_stocks(daily_df, threshold=29.0)
    if limit_up.empty:
        st.info("현재 상한가 종목이 없어 짝꿍 매매 시그널이 발생하지 않았습니다.")
        return

    if "업종" not in daily_df.columns:
        st.warning("업종 데이터가 없어 짝꿍 분석이 제한됩니다.")
        return

    signals = []
    for ticker, row in limit_up.iterrows():
        sector = row.get("업종", "")
        if not sector:
            continue
        # 같은 업종 내 2등주 찾기
        same_sector = daily_df[
            (daily_df["업종"] == sector)
            & (daily_df.index != ticker)
            & (daily_df["등락률"] > 0)
        ].sort_values("등락률", ascending=False)

        if same_sector.empty:
            continue

        runner = same_sector.iloc[0]
        runner_ticker = same_sector.index[0]

        signals.append({
            "leader_ticker": ticker,
            "leader_name": row.get("종목명", ticker),
            "leader_change": row.get("등락률", 0),
            "runner_ticker": runner_ticker,
            "runner_name": runner.get("종목명", runner_ticker),
            "runner_change": runner.get("등락률", 0),
            "runner_tv": runner.get("거래대금", 0) / 1e8,
            "sector": sector,
            "gap": row.get("등락률", 0) - runner.get("등락률", 0),
        })

    if not signals:
        st.info("짝꿍 매매 시그널이 없습니다.")
        return

    # 갭이 큰 순서대로 정렬 (1등주와 2등주 차이가 클수록 후발주 진입 매력도 높음)
    signals.sort(key=lambda x: x["gap"], reverse=True)

    st.markdown(f"**📊 짝꿍 매매 후보 {len(signals)}건 발견**")

    for sig in signals[:8]:
        gap = sig["gap"]
        # 갭에 따른 시그널 강도
        if gap >= 20:
            strength = "🔴 강력"
            strength_color = "#dc2626"
        elif gap >= 10:
            strength = "🟠 양호"
            strength_color = "#ea580c"
        else:
            strength = "🟡 관망"
            strength_color = "#f59e0b"

        r_chg = sig["runner_change"]
        r_color = "#dc2626" if r_chg > 0 else "#2563eb"

        st.markdown(
            f'<div style="background:#fff; border-radius:12px; padding:14px; margin-bottom:8px; '
            f'border:1px solid #e2e8f0; display:flex; align-items:center; gap:16px; flex-wrap:wrap;">'
            f'<div style="flex:0 0 auto;">'
            f'<span style="background:{strength_color}; color:#fff; padding:3px 10px; '
            f'border-radius:8px; font-size:0.72em; font-weight:700;">{strength}</span></div>'
            f'<div style="flex:1; min-width:120px;">'
            f'<div style="font-size:0.68em; color:#94a3b8;">🔒 대장주</div>'
            f'<div style="font-weight:700; color:#1e293b;">{sig["leader_name"]}</div>'
            f'<div style="font-weight:800; color:#dc2626;">+{sig["leader_change"]:.1f}%</div></div>'
            f'<div style="font-size:1.5em; color:#94a3b8;">→</div>'
            f'<div style="flex:1; min-width:120px;">'
            f'<div style="font-size:0.68em; color:#94a3b8;">🎯 후발주</div>'
            f'<div style="font-weight:700; color:#1e293b;">{sig["runner_name"]}</div>'
            f'<div style="font-weight:800; color:{r_color};">{"+" if r_chg > 0 else ""}{r_chg:.1f}%</div></div>'
            f'<div style="flex:0 0 auto; text-align:center;">'
            f'<div style="font-size:0.65em; color:#94a3b8;">갭</div>'
            f'<div style="font-size:1.1em; font-weight:700; color:{strength_color};">'
            f'{gap:.1f}%p</div></div>'
            f'<div style="flex:0 0 auto; text-align:center;">'
            f'<div style="font-size:0.65em; color:#94a3b8;">업종</div>'
            f'<div style="font-size:0.82em; color:#64748b;">{sig["sector"]}</div></div>'
            f'</div>',
            unsafe_allow_html=True,
        )
