"""
🌱 초기 발굴 AI - 막 움직이기 시작한 종목 조기 탐지

알고리즘:
- 3일 수익률 3~15% (본격 이동 전 초기 신호)
- 가격 박스권 유지 + 거래량 점진적 증가 (매집 패턴)
- 기관 10일 중 5일 이상 꾸준한 소량 순매수 (조용한 축적)

목적: 이미 급등한 종목이 아니라 "지금 막 준비 중인" 종목을 먼저 발굴
"""

import time
import datetime
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from data.fetcher import get_stock_ohlcv_history, get_investor_trend_individual
from analysis.scoring import calc_early_accumulation_score
from components.watchlist import get_watchlist, add_to_watchlist, remove_from_watchlist


# ---------------------------------------------------------------------------
# 내부 헬퍼
# ---------------------------------------------------------------------------

def _fetch_and_score_early(ticker: str, date: str, row: pd.Series) -> Optional[dict]:
    """개별 종목 OHLCV + 수급 fetch → 초기 발굴 점수 계산."""
    end_dt = datetime.datetime.strptime(date, "%Y%m%d")
    start_dt = end_dt - datetime.timedelta(days=80)
    start_str = start_dt.strftime("%Y%m%d")

    try:
        ohlcv = get_stock_ohlcv_history(ticker, start_str, date)
        investor = get_investor_trend_individual(ticker)
    except Exception:
        return None

    if ohlcv.empty or len(ohlcv) < 10:
        return None

    score, is_surged = calc_early_accumulation_score(ohlcv, investor)

    # 이미 급등했거나 점수 0이면 제외
    if is_surged or score <= 0:
        return None

    # 3일 수익률 계산 (표시용)
    close = ohlcv["종가"]
    ret_3d = (close.iloc[-1] / close.iloc[-4] - 1) * 100 if len(close) >= 4 else 0.0
    ret_5d = (close.iloc[-1] / close.iloc[-6] - 1) * 100 if len(close) >= 6 else 0.0

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
        "ret_3d": ret_3d,
        "ret_5d": ret_5d,
    }


def _ret_badge(ret_3d: float) -> str:
    """3일 수익률에 따른 색상 배지."""
    if 3.0 <= ret_3d <= 7.0:
        bg, fg = "#dcfce7", "#166534"
        label = f"▲ {ret_3d:.1f}% (매집초기)"
    elif 7.0 < ret_3d <= 15.0:
        bg, fg = "#fef9c3", "#854d0e"
        label = f"▲ {ret_3d:.1f}% (초기이동)"
    elif 0 < ret_3d < 3.0:
        bg, fg = "#f1f5f9", "#475569"
        label = f"▲ {ret_3d:.1f}% (대기중)"
    else:
        bg, fg = "#f1f5f9", "#475569"
        label = f"{ret_3d:+.1f}%"
    return (
        f'<span style="background:{bg}; color:{fg}; padding:2px 8px;'
        f' border-radius:8px; font-size:0.72em; font-weight:700;">{label}</span>'
    )


def _score_color(score: float) -> str:
    if score >= 70:
        return "#16a34a"
    elif score >= 45:
        return "#ca8a04"
    elif score >= 20:
        return "#2563eb"
    return "#94a3b8"


# ---------------------------------------------------------------------------
# 카드 렌더링
# ---------------------------------------------------------------------------

def _render_early_card(res: dict, rank: int):
    """초기 발굴 점수 카드."""
    score = res["score"]
    change = res["change"]
    price = res["price"]
    ret_3d = res["ret_3d"]

    price_color = "#dc2626" if change > 0 else "#2563eb" if change < 0 else "#94a3b8"
    arrow = "▲" if change > 0 else "▼" if change < 0 else "−"
    score_color = _score_color(score)

    rank_label = {1: "🌱", 2: "🌿", 3: "🍃"}.get(rank, f"#{rank}")

    market = res.get("market", "")
    mkt_color = "#1d4ed8" if market == "KOSPI" else "#16a34a" if market == "KOSDAQ" else "#94a3b8"
    mkt_badge = (
        f'<span style="background:{mkt_color}; color:white; padding:1px 6px;'
        f' border-radius:6px; font-size:0.72em; font-weight:700;">{market}</span> '
    ) if market else ""

    ret_badge = _ret_badge(ret_3d)

    inst_val = res["inst_5d"] / 1e8
    frgn_val = res["frgn_5d"] / 1e8
    inst_sign = "+" if inst_val > 0 else ""
    frgn_sign = "+" if frgn_val > 0 else ""

    html = (
        f'<div style="background:linear-gradient(135deg, #f0fdf4 0%, #ffffff 60%);'
        f' border-radius:16px; padding:18px;'
        f' border:1px solid #bbf7d0; box-shadow:0 4px 12px rgba(22,163,74,0.10);">'
        # 랭크 + 배지
        f'<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">'
        f'<span style="font-size:1.4em;">{rank_label}</span>'
        f'{ret_badge}'
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
        # 초기 발굴 점수
        f'<div style="margin-top:10px; text-align:center;">'
        f'<span style="font-size:2em; font-weight:800; color:{score_color};">{score:.1f}</span>'
        f'<span style="font-size:0.85em; color:#94a3b8;"> / 100</span>'
        f'</div>'
        f'<div style="font-size:0.68em; color:#64748b; text-align:center; margin-top:2px;">'
        f'초기 발굴 점수</div>'
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
# 주가 차트 (미니 버전)
# ---------------------------------------------------------------------------

def _render_early_chart(res: dict):
    """주가 + 거래량 미니 차트 (2패널)."""
    ohlcv = res["ohlcv"]
    name = res["name"]

    if ohlcv.empty or "종가" not in ohlcv.columns:
        return

    close = ohlcv["종가"].tail(30)
    volume = ohlcv["거래량"].tail(30) if "거래량" in ohlcv.columns else None

    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.65, 0.35],
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=[f"{name} 주가 (30일)", "거래량"],
    )

    # 주가선
    fig.add_trace(go.Scatter(
        x=close.index, y=close,
        name="종가", line=dict(color="#16a34a", width=2),
        fill="tozeroy", fillcolor="rgba(22,163,74,0.06)",
    ), row=1, col=1)

    # MA5
    if len(close) >= 5:
        ma5 = close.rolling(5, min_periods=1).mean()
        fig.add_trace(go.Scatter(
            x=ma5.index, y=ma5,
            name="MA5", line=dict(color="#7c3aed", width=1.2, dash="dot"),
        ), row=1, col=1)

    # 거래량 막대
    if volume is not None:
        avg_vol = volume.mean()
        bar_colors = [
            "#16a34a" if v >= avg_vol * 1.3 else "#94a3b8"
            for v in volume
        ]
        fig.add_trace(go.Bar(
            x=volume.index, y=volume,
            name="거래량", marker_color=bar_colors, opacity=0.75,
        ), row=2, col=1)

        # 20일 평균 거래량 기준선
        fig.add_hline(
            y=avg_vol, line_dash="dot",
            line_color="#ea580c", line_width=1,
            annotation_text="20일 평균",
            annotation_position="bottom right",
            row=2, col=1,
        )

    fig.update_layout(
        height=360,
        margin=dict(l=0, r=0, t=36, b=0),
        plot_bgcolor="#f8f9fc",
        paper_bgcolor="#ffffff",
        font=dict(size=11),
        showlegend=False,
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="#e2e8f0")

    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# 메인 렌더링 함수
# ---------------------------------------------------------------------------

def render_early_discovery_top3(daily_df: pd.DataFrame, date: str):
    """
    초기 발굴 AI TOP3 렌더링.

    후보 필터:
    - 등락률 +1% ~ +20% (너무 안 오른 것 / 이미 폭발한 것 제외)
    - 기관 or 외인 순매수 양수 (수급 확인)
    - 상위 40종목 스캔

    점수 기준 (calc_early_accumulation_score):
    - 3~15% 초기 이동 보너스
    - 박스권 + 거래량 증가 패턴
    - 기관 10일 꾸준한 매수
    """
    st.markdown("## 🌱 초기 발굴 AI TOP 3")
    st.caption(
        "3일 수익률 3~15% · 거래량 점진적 증가 · 기관 조용한 매수  |  "
        "지금 막 움직이기 시작한 종목 선별"
    )

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    # 후보 필터: 등락률 +1% ~ +20% + 수급 양수
    change_col = daily_df.get("등락률", pd.Series(dtype=float)).fillna(0)
    has_inst = daily_df.get("기관합계_5일", pd.Series(dtype=float)).fillna(0)
    has_frgn = daily_df.get("외국인합계_5일", pd.Series(dtype=float)).fillna(0)

    move_mask = (change_col >= 1.0) & (change_col <= 20.0)
    supply_mask = (has_inst > 0) | (has_frgn > 0)
    candidates = daily_df[move_mask & supply_mask].copy()

    if candidates.empty:
        st.info("조건에 맞는 후보 종목이 없습니다. (당일 등락률 1~20% + 수급 양수 기준)")
        return

    # 수급 합계 상위 40개 스캔
    candidates["_supply_sum"] = has_inst[move_mask & supply_mask] + has_frgn[move_mask & supply_mask]
    pool = candidates.nlargest(40, "_supply_sum")

    with st.spinner("🌱 초기 이동 종목 발굴 중… (최대 40종목 스캔)"):
        results = []
        for ticker, row in pool.iterrows():
            res = _fetch_and_score_early(ticker, date, row)
            if res is not None:
                results.append(res)
            time.sleep(0.08)

    if not results:
        st.warning("초기 발굴 조건을 충족하는 종목이 없습니다.")
        return

    # 점수 내림차순 정렬
    results.sort(key=lambda x: x["score"], reverse=True)
    top3 = results[:3]

    # ── TOP3 카드 ──
    card_cols = st.columns(3)
    for i, res in enumerate(top3):
        with card_cols[i]:
            _render_early_card(res, rank=i + 1)
            wl_tickers = {e["ticker"] for e in get_watchlist()}
            in_wl = res["ticker"] in wl_tickers
            btn_label = "⭐ 관심 해제" if in_wl else "☆ 관심종목 추가"
            if st.button(btn_label, key=f"wl_early_{res['ticker']}", use_container_width=True):
                if in_wl:
                    remove_from_watchlist(res["ticker"])
                else:
                    add_to_watchlist(
                        ticker=str(res["ticker"]),
                        name=str(res["name"]),
                        price=float(res["price"]),
                        sector=str(res["sector"]),
                        market=str(res.get("market", "")),
                        source="🌱 초기발굴",
                    )
                st.rerun()

    st.markdown("---")

    # ── 주가 차트 ──
    st.markdown("### 📊 초기 발굴 종목 차트")
    for res in top3:
        ret_str = f"+{res['ret_3d']:.1f}%" if res['ret_3d'] >= 0 else f"{res['ret_3d']:.1f}%"
        st.markdown(
            f'<div style="font-size:1.05em; font-weight:700; color:#1e293b;'
            f' margin:12px 0 4px;">{res["name"]} ({res["ticker"]})'
            f' &nbsp; <span style="font-size:0.85em; color:#16a34a;">'
            f'발굴점수 {res["score"]:.1f}</span>'
            f' &nbsp; <span style="font-size:0.8em; color:#ca8a04;">'
            f'3일 {ret_str}</span></div>',
            unsafe_allow_html=True,
        )
        _render_early_chart(res)
