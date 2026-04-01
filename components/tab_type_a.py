"""
🏆 Type A: 테마 1등주/서열 추격 (상따·준상따)
- 테마별 대장주 & 후발주 페어링
- 상한가 잠김 종목 → 같은 테마 후발주 매수 타점
- 테마 강도 & 서열 시각화
"""

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from data.fetcher import (
    get_theme_list,
    get_theme_constituents,
    detect_limit_up_stocks,
)
from logic_patterns import detect_theme_leaders, detect_limit_up_pairs
from components.watchlist import get_watchlist, add_to_watchlist, remove_from_watchlist


def render_tab_type_a(daily_df: pd.DataFrame, date_str: str):
    """Type A 탭 렌더링: 테마 1등주 추격."""
    st.markdown("## 🏆 Type A: 테마 1등주 / 서열 추격")
    st.caption("상따·준상따 · 테마 대장주 잠금 시 후발주 매수 · 체결강도 모니터링")

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    # ─── 상한가 종목 현황 ──────────────────────────────────────
    _render_limit_up_section(daily_df)

    st.markdown("---")

    # ─── 테마별 서열 추적 ──────────────────────────────────────
    _render_theme_ranking(daily_df)

    st.markdown("---")

    # ─── 짝꿍 매매 시그널 ──────────────────────────────────────
    _render_pair_signals(daily_df)


# ─────────────────────────────────────────────────────────────────────
def _render_limit_up_section(daily_df: pd.DataFrame):
    """상한가 · 급등 종목 현황."""
    st.markdown("### 🚀 상한가 · 급등 현황")

    limit_up = detect_limit_up_stocks(daily_df, threshold=29.0)
    near_limit = daily_df[
        (daily_df["등락률"] >= 20) & (daily_df["등락률"] < 29)
    ].sort_values("등락률", ascending=False) if "등락률" in daily_df.columns else pd.DataFrame()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("🔒 상한가", f"{len(limit_up)}개")
    with c2:
        st.metric("🔥 근접(20%+)", f"{len(near_limit)}개")
    with c3:
        st.metric("📊 합계", f"{len(limit_up) + len(near_limit)}개")

    if not limit_up.empty:
        _stock_card_grid(limit_up.head(10), "상한가", "#dc2626")

    if not near_limit.empty:
        with st.expander(f"🔥 근접 종목 ({len(near_limit)}개)"):
            _stock_card_grid(near_limit.head(10), "근접", "#ea580c")


def _stock_card_grid(df: pd.DataFrame, badge: str, color: str):
    """종목 카드 그리드."""
    cols = st.columns(min(5, max(1, len(df))))
    for i, row in enumerate(df.itertuples()):
        ticker = row.Index
        with cols[i % len(cols)]:
            name = getattr(row, '종목명', ticker) if hasattr(row, '종목명') else ticker
            change = getattr(row, '등락률', 0) if hasattr(row, '등락률') else 0
            price = getattr(row, '종가', 0) if hasattr(row, '종가') else 0
            tv = (getattr(row, '거래대금', 0) if hasattr(row, '거래대금') else 0) / 1e8
            sector = getattr(row, '업종', '') if hasattr(row, '업종') else ''
            market = getattr(row, '시장', '') if hasattr(row, '시장') else ''
            if not isinstance(sector, str):
                sector = ""
            st.markdown(
                f'<div style="background:#fff; border-radius:12px; padding:10px; '
                f'border-left:4px solid {color}; margin-bottom:6px; '
                f'box-shadow:0 1px 4px rgba(0,0,0,0.05);">'
                f'<span style="background:{color}; color:#fff; padding:1px 6px; '
                f'border-radius:5px; font-size:0.6em; font-weight:700;">{badge}</span>'
                f'<div style="font-weight:700; font-size:0.9em; margin:3px 0;">{name}</div>'
                f'<div style="font-size:1em; font-weight:800; color:#dc2626;">+{change:.1f}%</div>'
                f'<div style="font-size:0.7em; color:#64748b;">거래대금 {tv:,.0f}억</div>'
                f'{"<div style=font-size:0.65em;color:#94a3b8;>" + sector + "</div>" if sector else ""}'
                f'</div>',
                unsafe_allow_html=True,
            )
            wl_tickers = {e["ticker"] for e in get_watchlist()}
            in_wl = str(ticker) in wl_tickers
            btn_label = "⭐ 관심 해제" if in_wl else "☆ 관심종목 추가"
            if st.button(btn_label, key=f"wl_typeA_{badge}_{i}_{ticker}", use_container_width=True):
                if in_wl:
                    remove_from_watchlist(str(ticker))
                else:
                    add_to_watchlist(
                        ticker=str(ticker),
                        name=str(name),
                        price=float(price),
                        sector=str(sector),
                        market=str(market),
                        source="🏆 A:테마추격",
                    )
                st.rerun()


# ─────────────────────────────────────────────────────────────────────
def _render_theme_ranking(daily_df: pd.DataFrame):
    """테마별 대장주-후발주 서열 테이블."""
    st.markdown("### 📋 테마별 서열 추적기")

    leaders_df = detect_theme_leaders(daily_df)
    if leaders_df.empty:
        st.info("테마 서열 데이터를 구성할 수 없습니다.")
        return

    # 대장주만 필터
    top_themes = leaders_df[leaders_df["rank"] == 1].nlargest(10, "등락률")

    for _, row in top_themes.iterrows():
        theme = row["테마명"]
        same_theme = leaders_df[leaders_df["테마명"] == theme].sort_values("rank")

        with st.expander(f"📌 {theme} — 대장 {row['종목명']} (+{row['등락률']:.1f}%)", expanded=False):
            for _, member in same_theme.iterrows():
                is_leader = member["rank"] == 1
                icon = "🥇" if is_leader else "🥈"
                style = "font-weight:700;" if is_leader else ""
                chg_color = "#dc2626" if member["등락률"] > 0 else "#2563eb"
                tv = member["거래대금"] / 1e8
                st.markdown(
                    f'<div style="display:flex; justify-content:space-between; '
                    f'padding:4px 0; border-bottom:1px solid #f1f5f9; {style}">'
                    f'<span>{icon} {member["종목명"]} ({member["ticker"]})</span>'
                    f'<span style="color:{chg_color};">{member["등락률"]:+.1f}% · {tv:,.0f}억</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )


# ─────────────────────────────────────────────────────────────────────
def _render_pair_signals(daily_df: pd.DataFrame):
    """짝꿍 매매 시그널: 상한가 대장주 → 같은 업종 후발주."""
    st.markdown("### 🔀 짝꿍 매매 시그널")
    st.caption("상한가 종목의 같은 업종 내 후발 주자")

    pairs = detect_limit_up_pairs(daily_df)
    if pairs.empty:
        st.info("현재 상한가 종목이 없거나, 같은 업종 후발 후보가 없습니다.")
        return

    for pair_idx, (_, pair) in enumerate(pairs.iterrows()):
        chg_color = "#dc2626" if pair["후발_등락률"] > 0 else "#2563eb"
        follower_ticker = str(pair.get("후발_ticker", pair.get("후발_종목명", "")))
        follower_name = str(pair["후발_종목명"])
        col_card, col_btn = st.columns([5, 1])
        with col_card:
            st.markdown(
                f'<div style="background:#fff; border-radius:10px; padding:10px 14px; '
                f'border:1px solid #e2e8f0; margin-bottom:6px;">'
                f'<span style="font-size:0.72em; color:#94a3b8;">{pair["업종"]}</span>'
                f'<div style="display:flex; align-items:center; gap:8px; margin:4px 0;">'
                f'<span style="font-weight:700;">🔒 {pair["대장_종목명"]}</span>'
                f'<span style="color:#dc2626; font-weight:600;">+{pair["대장_등락률"]:.1f}%</span>'
                f'<span style="font-size:1.2em;">→</span>'
                f'<span style="font-weight:700;">🎯 {pair["후발_종목명"]}</span>'
                f'<span style="color:{chg_color}; font-weight:600;">{pair["후발_등락률"]:+.1f}%</span>'
                f'</div></div>',
                unsafe_allow_html=True,
            )
        with col_btn:
            if follower_ticker:
                wl_tickers = {e["ticker"] for e in get_watchlist()}
                in_wl = follower_ticker in wl_tickers
                btn_label = "⭐" if in_wl else "☆"
                if st.button(btn_label, key=f"wl_pair_{pair_idx}_{follower_ticker}", use_container_width=True):
                    if in_wl:
                        remove_from_watchlist(follower_ticker)
                    else:
                        add_to_watchlist(ticker=follower_ticker, name=follower_name, price=0.0, source="🏆 A:짝꾹후발주")
                    st.rerun()
