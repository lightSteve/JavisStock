"""
AI 스마트 Top 3 종목 컴포넌트

- 멀티팩터 점수(수급강도 40% + 가격모멘텀 30% + 거래량급증 30%) 기반 Top 3 선정
- 종목별 스코어 카드 및 수급 추이 Plotly 차트 렌더링
- 소외주 반등 감지 섹션
"""

import time
import datetime
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from data.fetcher import get_stock_ohlcv_history, get_investor_trend_individual
from analysis.scoring import calc_composite_score, is_anomaly_neglected_rebound
from components.watchlist import get_watchlist, add_to_watchlist, remove_from_watchlist


# ---------------------------------------------------------------------------
# 내부 헬퍼 함수
# ---------------------------------------------------------------------------

def _fetch_and_score(ticker: str, date: str, row: pd.Series) -> Optional[dict]:
    """개별 종목 OHLCV + 수급 데이터 fetch 후 점수 계산."""
    end_dt = datetime.datetime.strptime(date, "%Y%m%d")
    start_dt = end_dt - datetime.timedelta(days=80)
    start_str = start_dt.strftime("%Y%m%d")

    try:
        ohlcv = get_stock_ohlcv_history(ticker, start_str, date)
        investor = get_investor_trend_individual(ticker)
    except Exception:
        return None

    if ohlcv.empty or len(ohlcv) < 5:
        return None

    score, details = calc_composite_score(ohlcv, investor)

    return {
        "ticker": ticker,
        "name": str(row.get("종목명", ticker)),
        "price": float(row.get("종가", 0)),
        "change": float(row.get("등락률", 0)),
        "sector": str(row.get("업종", "")),
        "market": str(row.get("시장", "")),
        "inst_5d": float(row.get("기관합계_5일", 0)),
        "frgn_5d": float(row.get("외국인합계_5일", 0)),
        "ohlcv": ohlcv,
        "investor": investor,
        "score": score,
        "details": details,
    }


# ---------------------------------------------------------------------------
# 카드 렌더링
# ---------------------------------------------------------------------------

def _score_color(score: float) -> str:
    if score >= 75:
        return "#16a34a"
    elif score >= 50:
        return "#2563eb"
    elif score >= 25:
        return "#ea580c"
    return "#94a3b8"


def _render_score_card(res: dict, rank: int):
    """종합 점수 카드."""
    score = res["score"]
    details = res["details"]
    change = res["change"]
    price = res["price"]

    price_color = "#dc2626" if change > 0 else "#2563eb" if change < 0 else "#94a3b8"
    arrow = "▲" if change > 0 else "▼" if change < 0 else "−"
    score_color = _score_color(score)

    rank_colors = {1: "#f59e0b", 2: "#9ca3af", 3: "#b45309"}
    rank_color = rank_colors.get(rank, "#7c3aed")
    rank_label = {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank, f"#{rank}")

    anomaly_badge = (
        '<span style="background:#fef3c7; color:#92400e; padding:2px 7px;'
        ' border-radius:8px; font-size:0.68em; font-weight:700;">🔍 소외주반등</span>'
        if details.get("소외주반등") else ""
    )

    inst_val = res["inst_5d"] / 1e8
    frgn_val = res["frgn_5d"] / 1e8
    inst_sign = "+" if inst_val > 0 else ""
    frgn_sign = "+" if frgn_val > 0 else ""

    # score bar segments
    w_inst = details["수급강도점수"] * 0.4
    w_mom = details["가격모멘텀점수"] * 0.3
    w_vol = details["거래량급증점수"] * 0.3

    score_breakdown = (
        f'<div style="margin-top:8px;">'
        f'<div style="font-size:0.68em; color:#64748b; margin-bottom:3px;">종합 점수 구성</div>'
        f'<div style="display:flex; gap:3px; align-items:center;">'
        f'<div title="수급강도" style="height:8px; width:{w_inst:.1f}%; background:#2563eb; border-radius:3px;"></div>'
        f'<div title="모멘텀" style="height:8px; width:{w_mom:.1f}%; background:#7c3aed; border-radius:3px;"></div>'
        f'<div title="거래량" style="height:8px; width:{w_vol:.1f}%; background:#ea580c; border-radius:3px;"></div>'
        f'</div>'
        f'<div style="font-size:0.65em; color:#94a3b8; margin-top:3px;">'
        f'수급 {details["수급강도점수"]:.0f} / 모멘텀 {details["가격모멘텀점수"]:.0f} / 거래량 {details["거래량급증점수"]:.0f}'
        f'</div>'
        f'</div>'
    )

    market = res.get("market", "")
    mkt_color = "#1d4ed8" if market == "KOSPI" else "#16a34a" if market == "KOSDAQ" else "#94a3b8"
    mkt_badge = (f'<span style="background:{mkt_color}; color:white; padding:1px 6px;'
                 f' border-radius:6px; font-size:0.72em; font-weight:700;">{market}</span> ') if market else ""

    html = (
        f'<div style="background:#ffffff; border-radius:16px; padding:18px;'
        f' border:1px solid #e2e8f0; box-shadow:0 4px 12px rgba(0,0,0,0.08);">'
        # 랭크 + 배지
        f'<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">'
        f'<span style="font-size:1.4em;">{rank_label}</span>'
        f'{anomaly_badge}'
        f'</div>'
        # 티커 + 시장 + 섹터
        f'<div style="font-size:0.72em; color:#94a3b8; margin-bottom:4px;">'
        f'{mkt_badge}{res["ticker"]} &nbsp;|&nbsp; {res["sector"]}'
        f'</div>'
        # 종목명
        f'<div style="font-size:1.1em; font-weight:bold; color:#1e293b; margin-bottom:6px;">'
        f'{res["name"]}'
        f'</div>'
        # 가격
        f'<div style="font-size:1.25em; font-weight:bold; color:{price_color};">'
        f'{price:,.0f}원 '
        f'<span style="font-size:0.65em;">{arrow} {abs(change):.2f}%</span>'
        f'</div>'
        # 종합 점수
        f'<div style="margin-top:10px; text-align:center;">'
        f'<span style="font-size:2em; font-weight:800; color:{score_color};">{score:.1f}</span>'
        f'<span style="font-size:0.85em; color:#94a3b8;"> / 100</span>'
        f'</div>'
        # 점수 구성 막대
        f'{score_breakdown}'
        # 수급 요약
        f'<div style="margin-top:10px; font-size:0.75em;">'
        f'<span style="color:#2563eb; font-weight:700;">🏛️ {inst_sign}{inst_val:,.1f}억</span>'
        f'&nbsp;&nbsp;'
        f'<span style="color:#ea580c; font-weight:700;">🌍 {frgn_sign}{frgn_val:,.1f}억</span>'
        f'</div>'
        f'</div>'
    )
    st.markdown(html, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Plotly 차트
# ---------------------------------------------------------------------------

def _render_supply_chart(res: dict):
    """수급 추이 + 주가 Plotly 차트."""
    investor = res["investor"]
    ohlcv = res["ohlcv"]
    name = res["name"]
    ticker = res["ticker"]

    if investor.empty and ohlcv.empty:
        return

    # ── 서브플롯: 위=주가선, 아래=수급 막대 ──
    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.55, 0.45],
        shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=[f"{name} 주가 & VWAP", "투자자별 순매수 (억원)"],
    )

    # ── 위 패널: 종가 + MA5 + VWAP ──
    if not ohlcv.empty and "종가" in ohlcv.columns:
        close = ohlcv["종가"]
        volume = ohlcv["거래량"] if "거래량" in ohlcv.columns else pd.Series(dtype=float)

        fig.add_trace(go.Scatter(
            x=close.index, y=close,
            name="종가", line=dict(color="#1e293b", width=1.8),
        ), row=1, col=1)

        if len(close) >= 5:
            ma5 = close.rolling(5, min_periods=1).mean()
            fig.add_trace(go.Scatter(
                x=ma5.index, y=ma5,
                name="MA5", line=dict(color="#7c3aed", width=1.2, dash="dot"),
            ), row=1, col=1)

        if not volume.empty and len(close) >= 5:
            window = min(20, len(close))
            pv = (close * volume).tail(window)
            vs = volume.tail(window)
            vwap_val = pv.sum() / vs.sum() if vs.sum() > 0 else None
            if vwap_val:
                fig.add_hline(
                    y=vwap_val, line_dash="dash",
                    line_color="#ea580c", line_width=1.2,
                    annotation_text=f"VWAP {vwap_val:,.0f}",
                    annotation_position="bottom right",
                    row=1, col=1,
                )

    # ── 아래 패널: 기관 / 외국인 / 개인 막대 ──
    if not investor.empty:
        inv = investor.copy()
        for col in ["기관합계", "외국인합계", "개인"]:
            if col not in inv.columns:
                inv[col] = 0
        inv = inv / 1e8  # 억원

        bar_cfg = [
            ("기관합계", "기관", "#2563eb"),
            ("외국인합계", "외국인", "#ea580c"),
            ("개인", "개인", "#16a34a"),
        ]
        for col, label, color in bar_cfg:
            fig.add_trace(go.Bar(
                x=inv.index, y=inv[col],
                name=label,
                marker_color=color,
                opacity=0.82,
            ), row=2, col=1)

    fig.update_layout(
        height=440,
        margin=dict(l=0, r=0, t=40, b=0),
        legend=dict(orientation="h", y=1.05, x=0),
        plot_bgcolor="#f8f9fc",
        paper_bgcolor="#ffffff",
        font=dict(size=11),
        barmode="overlay",
        showlegend=True,
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="#e2e8f0")

    st.plotly_chart(fig, use_container_width=True)


def _render_anomaly_card(res: dict):
    """소외주 반등 종목 컴팩트 표시."""
    change = res["change"]
    price_color = "#dc2626" if change > 0 else "#2563eb" if change < 0 else "#94a3b8"
    arrow = "▲" if change > 0 else "▼" if change < 0 else "−"
    score = res["score"]

    html = (
        f'<div style="background:#fefce8; border-left:4px solid #f59e0b;'
        f' border-radius:10px; padding:12px 16px; margin-bottom:8px;'
        f' display:flex; align-items:center; gap:16px;">'
        f'<span style="font-size:1.5em;">🔍</span>'
        f'<div style="flex:1;">'
        f'<div style="font-weight:700; color:#1e293b;">{res["name"]}'
        f' <span style="color:#94a3b8; font-size:0.8em;">({res["ticker"]})</span></div>'
        f'<div style="font-size:0.82em; color:#64748b;">{res["sector"]}</div>'
        f'</div>'
        f'<div style="text-align:right;">'
        f'<div style="font-weight:700; color:{price_color};">'
        f'{res["price"]:,.0f}원 {arrow} {abs(change):.2f}%'
        f'</div>'
        f'<div style="font-size:0.8em; color:#92400e; font-weight:600;">점수 {score:.1f}</div>'
        f'</div>'
        f'</div>'
    )
    st.markdown(html, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# 메인 렌더링 함수
# ---------------------------------------------------------------------------

def render_smart_top3(daily_df: pd.DataFrame, date: str, precomputed: list = None):
    """
    멀티팩터 점수 기반 Top 3 종목 카드 + 수급 추이 차트 렌더링.

    - 수급 양호 상위 30 종목 대상으로 점수 계산
    - Top 3 를 카드 + Plotly 차트로 표시
    - 소외주 반등 감지 종목 별도 알림
    - precomputed: 스케줄러가 사전 계산한 결과 리스트 (있으면 API 호출 스킵)
    """
    st.markdown("## 🏆 AI 스마트 TOP 3")
    st.caption("수급 강도 40% · 가격 모멘텀 30% · 거래량 급증 30% 가중 합산")

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    # 사전 계산된 결과가 있으면 바로 사용
    if precomputed:
        results = precomputed
        st.caption("⚡ 사전 분석 데이터 사용 (즉시 로드)")
    else:
        # 수급 후보 필터링 (기관 or 외인 순매수 양수)
        has_inst = daily_df.get("기관합계_5일", pd.Series(dtype=float)).fillna(0)
        has_frgn = daily_df.get("외국인합계_5일", pd.Series(dtype=float)).fillna(0)
        supply_mask = (has_inst > 0) | (has_frgn > 0)
        candidates = daily_df[supply_mask].copy()

        if candidates.empty:
            st.info("수급 양호 종목이 없습니다. 먼저 데이터를 로드해주세요.")
            return

        # 수급 합계 상위 30개만 점수 계산 (API 부하 방지)
        candidates["_supply_sum"] = has_inst[supply_mask] + has_frgn[supply_mask]
        pool = candidates.nlargest(30, "_supply_sum")

        with st.spinner("📊 멀티팩터 점수 계산 중… (최대 30종목 분석)"):
            results = []
            for ticker, row in pool.iterrows():
                res = _fetch_and_score(ticker, date, row)
                if res is not None:
                    results.append(res)
                time.sleep(0.1)

    if not results:
        st.warning("점수를 계산할 수 있는 종목이 없습니다.")
        return

    # 종합 점수 내림차순 정렬
    results.sort(key=lambda x: x["score"], reverse=True)
    top3 = results[:3]
    anomaly_stocks = [r for r in results if r["details"].get("소외주반등", False)]

    # ── Top 3 카드 ──
    card_cols = st.columns(3)
    for i, res in enumerate(top3):
        with card_cols[i]:
            _render_score_card(res, rank=i + 1)
            wl_tickers = {e["ticker"] for e in get_watchlist()}
            in_wl = res["ticker"] in wl_tickers
            btn_label = "⭐ 관심 해제" if in_wl else "☆ 관심종목 추가"
            if st.button(btn_label, key=f"wl_smart_{res['ticker']}", use_container_width=True):
                if in_wl:
                    remove_from_watchlist(res["ticker"])
                else:
                    add_to_watchlist(
                        ticker=str(res["ticker"]),
                        name=str(res["name"]),
                        price=float(res["price"]),
                        sector=str(res["sector"]),
                        market=str(res.get("market", "")),
                        source="🏆 AI Top3",
                    )
                st.rerun()

    st.markdown("---")

    # ── 수급 추이 차트 (Top 3 순서대로) ──
    st.markdown("### 📈 수급 추이 분석")
    for res in top3:
        st.markdown(
            f'<div style="font-size:1.05em; font-weight:700; color:#1e293b;'
            f' margin:12px 0 4px;">{res["name"]} ({res["ticker"]})'
            f' &nbsp; <span style="font-size:0.85em; color:#7c3aed;">'
            f'종합 {res["score"]:.1f}점</span></div>',
            unsafe_allow_html=True,
        )
        _render_supply_chart(res)

    # ── 소외주 반등 섹션 ──
    if anomaly_stocks:
        st.markdown("---")
        st.markdown("### 🔍 소외주 반등 감지")
        st.caption(
            "최근 10거래일 거래량이 20일 평균의 30% 미만이었다가 "
            "최근 3거래일 평균이 150% 이상으로 급증한 종목"
        )
        for res in anomaly_stocks[:5]:
            _render_anomaly_card(res)
