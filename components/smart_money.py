"""
💰 Panel 3: Smart Money Flow
(유형 C & F 공략: 돌파 및 수급 매매)
- 프로그램 순매수 추적기
- IPO/신규 상장 대어 모니터
- 다중 테마 중첩 필터
"""

import datetime
import time

import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go

from data.fetcher import (
    get_program_trading_top,
    get_theme_list,
    get_theme_constituents,
    get_stock_ohlcv_history,
    get_investor_trend_individual,
    detect_volume_spike_stocks,
)


# ═══════════════════════════════════════════════════════════════════════════
# 메인 진입점
# ═══════════════════════════════════════════════════════════════════════════

def render_smart_money(daily_df: pd.DataFrame, date_str: str):
    """Panel 3: Smart Money Flow 렌더링."""
    st.markdown("## 💰 Smart Money Flow")
    st.caption("프로그램 순매수 · IPO 대어 · 다중 테마 중첩")

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    tab_prog, tab_ipo, tab_multi = st.tabs([
        "📊 프로그램 순매수",
        "🆕 IPO/신규 대어",
        "🔗 다중 테마 중첩",
    ])

    with tab_prog:
        _render_program_trading(daily_df, date_str)

    with tab_ipo:
        _render_ipo_monitor(daily_df, date_str)

    with tab_multi:
        _render_multi_theme_overlap(daily_df)


# ═══════════════════════════════════════════════════════════════════════════
# 1) 프로그램 순매수 추적기
# ═══════════════════════════════════════════════════════════════════════════

def _render_program_trading(daily_df: pd.DataFrame, date_str: str):
    """프로그램 순매수 상위 + 전고점 돌파 필터."""
    st.markdown("### 📊 프로그램 순매수 추적기")
    st.caption("뉴스 없이 프로그램 매수가 선행되며 전고점 돌파를 시도하는 종목")

    with st.spinner("프로그램 매매 데이터 수집 중..."):
        prog_df = get_program_trading_top()

    if prog_df.empty:
        # 프로그램 데이터 없으면 기관 수급 기반 대체
        st.caption("⚠️ 프로그램 매매 데이터 수집 불가 시, 기관 순매수 기반으로 대체합니다.")
        _render_institutional_proxy(daily_df, date_str)
        return

    # 순매수 상위
    buy_df = prog_df[prog_df["프로그램순매수"] > 0].sort_values("프로그램순매수", ascending=False).head(15)
    sell_df = prog_df[prog_df["프로그램순매수"] < 0].sort_values("프로그램순매수", ascending=True).head(10)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("📈 프로그램 순매수 종목", f"{len(buy_df)}개")
    with c2:
        st.metric("📉 프로그램 순매도 종목", f"{len(sell_df)}개")

    # 순매수 TOP 바 차트
    if not buy_df.empty:
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

    # 전고점 돌파 필터 (프로그램 순매수 + 상승중 종목)
    st.markdown("#### 🎯 전고점 돌파 후보")
    _render_breakout_candidates(daily_df, buy_df, date_str)


def _render_institutional_proxy(daily_df: pd.DataFrame, date_str: str):
    """기관 순매수 기반 프로그램 매매 대체 분석."""
    if "기관합계_5일" not in daily_df.columns:
        st.info("수급 데이터가 없습니다.")
        return

    # 기관 순매수 상위 + 등락률 양수
    inst_top = daily_df[
        (daily_df["기관합계_5일"] > 0) & (daily_df["등락률"] > 0)
    ].nlargest(20, "기관합계_5일")

    if inst_top.empty:
        st.info("기관 순매수 종목이 없습니다.")
        return

    # 바 차트
    display = inst_top.head(15).copy()
    display["_억"] = (display["기관합계_5일"] / 1e8).round(1)
    display["_name"] = display["종목명"].str[:8] if "종목명" in display.columns else display.index

    fig = go.Figure(go.Bar(
        y=display["_name"][::-1],
        x=display["_억"][::-1],
        orientation="h",
        marker_color="#2563eb",
        text=display["_억"][::-1].apply(lambda x: f"{x:+,.0f}억"),
        textposition="outside",
    ))
    fig.update_layout(
        title="기관 순매수 TOP 15 (프로그램 매매 대체)",
        height=400,
        margin=dict(l=10, r=60, t=40, b=10),
        template="plotly_white",
    )
    st.plotly_chart(fig, use_container_width=True)

    # 전고점 돌파 후보
    st.markdown("#### 🎯 전고점 돌파 후보")
    _render_breakout_candidates(daily_df, inst_top, date_str)


def _render_breakout_candidates(daily_df: pd.DataFrame, candidate_df: pd.DataFrame, date_str: str):
    """전고점 돌파 후보 종목 분석."""
    if candidate_df.empty:
        st.info("분석 대상 종목이 없습니다.")
        return

    # 후보군에서 daily_df와 매칭
    tickers = []
    if "티커" in candidate_df.columns:
        tickers = candidate_df["티커"].tolist()
    else:
        tickers = candidate_df.index.tolist()

    # 상위 5개 종목만 OHLCV 분석 (성능)
    check_tickers = [t for t in tickers[:8] if t in daily_df.index]
    if not check_tickers:
        st.info("분석 대상 종목이 없습니다.")
        return

    breakout_results = []
    end_dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    start_dt = end_dt - datetime.timedelta(days=120)
    start_str = start_dt.strftime("%Y%m%d")

    progress = st.progress(0, text="전고점 돌파 분석 중...")
    for i, ticker in enumerate(check_tickers):
        progress.progress((i + 1) / len(check_tickers), text=f"분석 중: {ticker}")
        try:
            ohlcv = get_stock_ohlcv_history(ticker, start_str, date_str)
            if ohlcv.empty or len(ohlcv) < 20:
                continue

            current_price = ohlcv["종가"].iloc[-1]
            high_60d = ohlcv["고가"].tail(60).max()
            high_20d = ohlcv["고가"].tail(20).max()

            # 전고점 돌파 여부
            is_breakout_60 = current_price >= high_60d * 0.98
            is_breakout_20 = current_price >= high_20d * 0.98

            name = daily_df.loc[ticker, "종목명"] if ticker in daily_df.index else ticker
            change = daily_df.loc[ticker, "등락률"] if ticker in daily_df.index else 0

            breakout_results.append({
                "ticker": ticker,
                "name": name,
                "price": current_price,
                "change": change,
                "high_60d": high_60d,
                "high_20d": high_20d,
                "breakout_60": is_breakout_60,
                "breakout_20": is_breakout_20,
                "proximity_60": (current_price / high_60d * 100) if high_60d > 0 else 0,
            })
        except Exception:
            continue
        time.sleep(0.1)
    progress.empty()

    if not breakout_results:
        st.info("전고점 돌파 후보가 없습니다.")
        return

    for res in breakout_results:
        is_break = res["breakout_60"]
        badge = "🔥 60일 전고점 돌파" if is_break else f"📊 전고점 대비 {res['proximity_60']:.1f}%"
        badge_bg = "#dcfce7" if is_break else "#f8fafc"
        badge_color = "#16a34a" if is_break else "#64748b"
        chg = res["change"]
        chg_color = "#dc2626" if chg > 0 else "#2563eb" if chg < 0 else "#6b7280"

        st.markdown(
            f'<div style="background:#fff; border-radius:10px; padding:12px; margin-bottom:6px; '
            f'border:1px solid #e2e8f0; display:flex; align-items:center; gap:14px; flex-wrap:wrap;">'
            f'<div style="flex:1; min-width:100px;">'
            f'<div style="font-weight:700; color:#1e293b;">{res["name"]}'
            f'<span style="font-size:0.72em; color:#94a3b8; margin-left:6px;">{res["ticker"]}</span></div>'
            f'<div style="font-weight:700; color:{chg_color};">{"+" if chg > 0 else ""}{chg:.2f}%</div></div>'
            f'<div style="flex:0 0 auto;">'
            f'<span style="background:{badge_bg}; color:{badge_color}; padding:3px 10px; '
            f'border-radius:8px; font-size:0.75em; font-weight:600;">{badge}</span></div>'
            f'<div style="flex:0 0 auto; text-align:center;">'
            f'<div style="font-size:0.65em; color:#94a3b8;">60일 고점</div>'
            f'<div style="font-size:0.9em; color:#1e293b;">{res["high_60d"]:,.0f}원</div></div>'
            f'</div>',
            unsafe_allow_html=True,
        )


# ═══════════════════════════════════════════════════════════════════════════
# 2) IPO / 신규 상장 대어 모니터
# ═══════════════════════════════════════════════════════════════════════════

def _render_ipo_monitor(daily_df: pd.DataFrame, date_str: str):
    """신규 상장주 중 기관 연속 순매수 포착."""
    st.markdown("### 🆕 IPO / 신규 상장 대어 모니터")
    st.caption("상장 초기 기관(연기금 등)의 연속 순매수가 포착되는 신규 상장주")

    if "기관합계_5일" not in daily_df.columns:
        st.info("수급 데이터가 없습니다.")
        return

    # 신규 상장주 탐지: OHLCV 히스토리가 짧은 종목 + 기관 순매수
    # 거래대금 상위 + 기관 순매수 양수 종목 중 OHLCV가 짧은 종목
    candidates = daily_df[
        (daily_df["기관합계_5일"] > 0)
        & (daily_df["등락률"].notna())
    ].nlargest(50, "기관합계_5일")

    if candidates.empty:
        st.info("분석 대상 종목이 없습니다.")
        return

    ipo_results = []
    end_dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    start_dt = end_dt - datetime.timedelta(days=200)
    start_str = start_dt.strftime("%Y%m%d")

    # 상위 15개만 체크 (성능)
    check_list = candidates.head(15)

    progress = st.progress(0, text="신규 상장 여부 확인 중...")
    for idx, (ticker, row) in enumerate(check_list.iterrows()):
        progress.progress((idx + 1) / len(check_list), text=f"확인 중: {row.get('종목명', ticker)}")
        try:
            ohlcv = get_stock_ohlcv_history(ticker, start_str, date_str)
            if ohlcv.empty:
                continue

            trading_days = len(ohlcv)

            # 상장 60거래일 이내를 신규 상장으로 판단
            if trading_days <= 60:
                # 기관 투자자 동향
                inv = get_investor_trend_individual(ticker)
                inst_consecutive = 0
                if not inv.empty and "기관합계" in inv.columns:
                    for val in inv["기관합계"].values:
                        if val > 0:
                            inst_consecutive += 1
                        else:
                            break

                ipo_results.append({
                    "ticker": ticker,
                    "name": row.get("종목명", ticker),
                    "price": row.get("종가", 0),
                    "change": row.get("등락률", 0),
                    "inst_5d": row.get("기관합계_5일", 0),
                    "frgn_5d": row.get("외국인합계_5일", 0),
                    "trading_days": trading_days,
                    "inst_consecutive": inst_consecutive,
                    "sector": row.get("업종", ""),
                })
        except Exception:
            continue
        time.sleep(0.1)
    progress.empty()

    if not ipo_results:
        st.info("📭 최근 상장된 종목 중 기관 순매수 포착 건이 없습니다.")
        st.caption("기관이 순매수 중인 신규 상장주를 모니터링합니다.")
        return

    # 기관 연속 순매수일 기준 정렬
    ipo_results.sort(key=lambda x: x["inst_consecutive"], reverse=True)

    st.markdown(f"**🎯 신규 상장 + 기관 매수 포착: {len(ipo_results)}건**")

    for res in ipo_results:
        inst_억 = res["inst_5d"] / 1e8
        frgn_억 = res["frgn_5d"] / 1e8
        consec = res["inst_consecutive"]
        chg = res["change"]
        chg_color = "#dc2626" if chg > 0 else "#2563eb" if chg < 0 else "#6b7280"

        # 연속 순매수 강도 뱃지
        if consec >= 4:
            strength_badge = "🔴 강력 매집"
            strength_bg = "#fee2e2"
            strength_color = "#dc2626"
        elif consec >= 2:
            strength_badge = "🟠 매집 진행"
            strength_bg = "#ffedd5"
            strength_color = "#ea580c"
        else:
            strength_badge = "🟡 초기 진입"
            strength_bg = "#fef3c7"
            strength_color = "#f59e0b"

        st.markdown(
            f'<div style="background:#fff; border-radius:12px; padding:14px; margin-bottom:8px; '
            f'border:1px solid #e2e8f0; box-shadow:0 1px 4px rgba(0,0,0,0.04);">'
            f'<div style="display:flex; justify-content:space-between; align-items:center; '
            f'flex-wrap:wrap; gap:8px;">'
            # 종목 정보
            f'<div style="flex:1; min-width:140px;">'
            f'<div style="font-weight:700; font-size:1.05em; color:#1e293b;">{res["name"]}'
            f'<span style="font-size:0.7em; color:#94a3b8; margin-left:6px;">{res["ticker"]}</span></div>'
            f'<div style="font-size:0.78em; color:#64748b;">{res["sector"]} · 상장 {res["trading_days"]}거래일</div>'
            f'</div>'
            # 가격/등락
            f'<div style="text-align:center;">'
            f'<div style="font-weight:700; color:{chg_color};">{"+" if chg > 0 else ""}{chg:.2f}%</div>'
            f'<div style="font-size:0.78em; color:#64748b;">{res["price"]:,.0f}원</div></div>'
            # 수급
            f'<div style="text-align:center;">'
            f'<div style="font-size:0.72em; color:#94a3b8;">기관/외국인</div>'
            f'<div style="font-size:0.85em;">'
            f'<span style="color:#2563eb; font-weight:600;">🏛️ {inst_억:+,.0f}억</span>'
            f' <span style="color:#ea580c; font-weight:600;">🌍 {frgn_억:+,.0f}억</span></div></div>'
            # 연속 순매수
            f'<div style="text-align:center;">'
            f'<span style="background:{strength_bg}; color:{strength_color}; padding:4px 10px; '
            f'border-radius:8px; font-size:0.75em; font-weight:700;">{strength_badge}</span>'
            f'<div style="font-size:0.68em; color:#94a3b8; margin-top:3px;">연속 {consec}일 순매수</div></div>'
            f'</div></div>',
            unsafe_allow_html=True,
        )


# ═══════════════════════════════════════════════════════════════════════════
# 3) 다중 테마 중첩 필터
# ═══════════════════════════════════════════════════════════════════════════

def _render_multi_theme_overlap(daily_df: pd.DataFrame):
    """두 가지 이상의 강한 테마가 공존하며 급등하는 종목."""
    st.markdown("### 🔗 다중 테마 중첩 필터")
    st.caption("두 가지 이상의 강한 테마에 동시 탑재되며 급등하는 '특이 현상' 종목")

    with st.spinner("테마 중첩 분석 중..."):
        theme_df = get_theme_list()

    if theme_df.empty:
        # 업종 + 거래대금 급등 기반 대체
        _render_volume_spike_proxy(daily_df)
        return

    # 상위 10개 테마 구성 종목 수집
    top_themes = theme_df.nlargest(10, "등락률")
    ticker_to_themes = {}  # ticker -> [테마1, 테마2, ...]

    for _, theme in top_themes.iterrows():
        theme_name = theme["테마명"]
        theme_no = str(theme.get("테마번호", ""))
        if not theme_no:
            continue

        const_df = get_theme_constituents(theme_no)
        if const_df.empty:
            continue

        for _, stock in const_df.iterrows():
            ticker = stock["티커"]
            if ticker not in ticker_to_themes:
                ticker_to_themes[ticker] = []
            ticker_to_themes[ticker].append(theme_name)

    # 2개 이상 테마에 속한 종목 필터
    multi_theme_stocks = {
        ticker: themes
        for ticker, themes in ticker_to_themes.items()
        if len(themes) >= 2
    }

    if not multi_theme_stocks:
        st.info("📭 현재 다중 테마 중첩 종목이 없습니다.")
        _render_volume_spike_proxy(daily_df)
        return

    # daily_df와 매칭
    results = []
    for ticker, themes in multi_theme_stocks.items():
        if ticker not in daily_df.index:
            continue
        row = daily_df.loc[ticker]
        change = row.get("등락률", 0)
        if change <= 0:
            continue  # 상승 종목만

        results.append({
            "ticker": ticker,
            "name": row.get("종목명", ticker),
            "price": row.get("종가", 0),
            "change": change,
            "themes": themes,
            "theme_count": len(themes),
            "tv": row.get("거래대금", 0),
            "inst_5d": row.get("기관합계_5일", 0) if "기관합계_5일" in row.index else 0,
        })

    results.sort(key=lambda x: (-x["theme_count"], -x["change"]))

    st.markdown(f"**🔥 다중 테마 중첩 종목: {len(results)}건**")

    if not results:
        st.info("상승 중인 다중 테마 종목이 없습니다.")
        return

    for res in results[:12]:
        themes_str = " · ".join(f"#{t}" for t in res["themes"])
        chg = res["change"]
        chg_color = "#dc2626" if chg > 0 else "#2563eb"
        tv_억 = res["tv"] / 1e8
        inst_억 = res["inst_5d"] / 1e8

        overlap_badge_color = "#7c3aed" if res["theme_count"] >= 3 else "#2563eb"

        st.markdown(
            f'<div style="background:#fff; border-radius:12px; padding:14px; margin-bottom:8px; '
            f'border:1px solid #e2e8f0;">'
            f'<div style="display:flex; justify-content:space-between; align-items:center; '
            f'flex-wrap:wrap; gap:8px; margin-bottom:8px;">'
            f'<div>'
            f'<span style="background:{overlap_badge_color}; color:#fff; padding:2px 8px; '
            f'border-radius:6px; font-size:0.68em; font-weight:700;">'
            f'🔗 {res["theme_count"]}중 테마</span></div>'
            f'<div style="font-weight:800; color:{chg_color}; font-size:1.1em;">'
            f'{"+" if chg > 0 else ""}{chg:.2f}%</div></div>'
            f'<div style="font-weight:700; font-size:1.05em; color:#1e293b;">{res["name"]}'
            f'<span style="font-size:0.72em; color:#94a3b8; margin-left:6px;">{res["ticker"]}</span></div>'
            f'<div style="font-size:0.78em; color:#7c3aed; margin-top:4px;">{themes_str}</div>'
            f'<div style="font-size:0.75em; color:#64748b; margin-top:6px;">'
            f'거래대금 {tv_억:,.0f}억 · 🏛️기관 {inst_억:+,.0f}억</div>'
            f'</div>',
            unsafe_allow_html=True,
        )


def _render_volume_spike_proxy(daily_df: pd.DataFrame):
    """테마 데이터 없을 때 거래대금 급등 기반 대체."""
    st.markdown("---")
    st.caption("⚠️ 테마 데이터 불가 시, 거래대금 급등 + 다중 조건 충족 종목으로 대체합니다.")

    spike_df = detect_volume_spike_stocks(daily_df, min_change=5.0)
    if spike_df.empty:
        st.info("거래대금 급등 종목이 없습니다.")
        return

    # 기관+외국인 쌍끌이 필터
    if "기관합계_5일" in spike_df.columns and "외국인합계_5일" in spike_df.columns:
        dual = spike_df[
            (spike_df["기관합계_5일"] > 0) & (spike_df["외국인합계_5일"] > 0)
        ]
        if not dual.empty:
            spike_df = dual

    st.markdown(f"**📊 거래대금 급등 + 수급 양호 종목: {len(spike_df.head(10))}건**")

    for ticker, row in spike_df.head(10).iterrows():
        name = row.get("종목명", ticker)
        chg = row.get("등락률", 0)
        tv = row.get("거래대금", 0) / 1e8
        chg_color = "#dc2626" if chg > 0 else "#2563eb"

        st.markdown(
            f'<div style="background:#f8fafc; border-radius:8px; padding:10px; margin-bottom:4px; '
            f'border:1px solid #e2e8f0; display:flex; justify-content:space-between; align-items:center;">'
            f'<div><span style="font-weight:700; color:#1e293b;">{name}</span>'
            f'<span style="font-size:0.72em; color:#94a3b8; margin-left:6px;">{ticker}</span></div>'
            f'<div style="text-align:right;">'
            f'<span style="font-weight:700; color:{chg_color};">{"+" if chg > 0 else ""}{chg:.2f}%</span>'
            f'<span style="font-size:0.78em; color:#64748b; margin-left:8px;">거래대금 {tv:,.0f}억</span>'
            f'</div></div>',
            unsafe_allow_html=True,
        )
