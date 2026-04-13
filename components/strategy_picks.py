"""
📋 전략별 종목추천 컴포넌트

7가지 매매 전략별 오늘의 추천 종목을 카드/테이블로 렌더링.
"""

import streamlit as st
import pandas as pd
from typing import Dict, List

from components.watchlist import get_watchlist, add_to_watchlist, remove_from_watchlist
from logic_strategies import (
    ALL_STRATEGIES,
    screen_scalp_breakout,
    screen_close_betting,
    screen_abcd_pattern,
    screen_pullback_buy,
    screen_limit_up_follow,
    screen_institutional_flow,
    screen_swing_trade,
)


# ═══════════════════════════════════════════════════════════════════════════
# 메인 렌더링
# ═══════════════════════════════════════════════════════════════════════════

@st.fragment
def render_strategy_picks(daily_df: pd.DataFrame, date_str: str):
    """7가지 전략별 추천 종목 탭 렌더링."""
    st.markdown("## 📋 전략별 종목추천")
    st.caption("7가지 매매 전략 · 오늘 종가 기준 자동 스크리닝 · 각 전략 Top 5")

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    # ── 전략 선택 서브탭 ──
    tab_labels = [f'{s["icon"]} {s["name"]}' for s in ALL_STRATEGIES]
    tabs = st.tabs(tab_labels)

    # 전략별 실행 매핑
    strategy_funcs = {
        "scalp": (screen_scalp_breakout, False),
        "close_bet": (screen_close_betting, False),
        "abcd": (screen_abcd_pattern, True),
        "pullback": (screen_pullback_buy, True),
        "limit_follow": (screen_limit_up_follow, False),
        "inst_flow": (screen_institutional_flow, True),
        "swing": (screen_swing_trade, True),
    }

    for i, strat in enumerate(ALL_STRATEGIES):
        with tabs[i]:
            _render_strategy_section(
                daily_df, date_str, strat, strategy_funcs[strat["key"]]
            )


@st.fragment
def _render_strategy_section(daily_df: pd.DataFrame, date_str: str,
                              strat: dict, func_info: tuple):
    """개별 전략 섹션 렌더링."""
    fn, needs_date = func_info
    key = strat["key"]

    # 전략 설명
    _render_strategy_desc(strat)

    # 실행 버튼
    run_key = f"strat_run_{key}"
    result_key = f"strat_result_{key}"

    if st.button(f'🔍 {strat["name"]} 스크리닝 실행', key=run_key, use_container_width=True):
        with st.spinner(f'{strat["icon"]} {strat["name"]} 분석 중...'):
            if needs_date:
                results = fn(daily_df, date_str, 5)
            else:
                results = fn(daily_df, 5)
        st.session_state[result_key] = results

    # 결과 표시
    results = st.session_state.get(result_key, None)
    if results is None:
        st.info("위 버튼을 눌러 스크리닝을 실행하세요.")
        return

    if not results:
        st.warning("조건에 맞는 종목이 없습니다.")
        return

    st.markdown(f"### {strat['icon']} Top {len(results)} 추천 종목")

    # 카드 렌더링
    _render_result_cards(results, strategy_key=key)

    # 테이블 렌더링
    with st.expander("📊 상세 데이터 테이블"):
        _render_result_table(results)


# ═══════════════════════════════════════════════════════════════════════════
# 전략 설명
# ═══════════════════════════════════════════════════════════════════════════

_STRATEGY_DESC = {
    "scalp": {
        "title": "스캘/돌파매매",
        "desc": "종가가 당일 고가의 95% 이상으로 강하게 마감 + 거래량 급증 + 양봉",
        "criteria": ["종가/고가 ≥ 95%", "등락률 > +2%", "양봉 확인", "거래대금 상위"],
        "color": "#dc2626",
    },
    "close_bet": {
        "title": "종가베팅",
        "desc": "장 마감 시점에 종가가 고가와 거의 동일 (1% 이내) + 거래대금 상위 20%",
        "criteria": ["고가-종가 ≤ 1%", "양봉 확인", "거래대금 상위 20%", "등락률 0.5~15%"],
        "color": "#ea580c",
    },
    "abcd": {
        "title": "ABCD 패턴매매",
        "desc": "20일 고점 대비 -5~-15% 조정 후 금일 양봉 반등 + MACD 전환",
        "criteria": ["20일 고점 대비 -5~-15%", "금일 양봉 반등", "MACD 매수전환/하락둔화"],
        "color": "#7c3aed",
    },
    "pullback": {
        "title": "눌림목매매",
        "desc": "정배열 상태에서 MA5 지지선 눌림 후 반등 + RSI 적정 구간 + 수급",
        "criteria": ["정배열(초기 이상)", "MA5 ±1.5% 이내", "RSI 35~62", "기관/외인 매수"],
        "color": "#2563eb",
    },
    "limit_follow": {
        "title": "상한가따라잡기",
        "desc": "상한가(+29%↑) 종목의 동일 업종 내 +5~20% 후발주 포착",
        "criteria": ["업종 내 상한가 존재", "후발주 +5~20%", "거래대금 상위"],
        "color": "#dc2626",
    },
    "inst_flow": {
        "title": "기관/외인 수급매매",
        "desc": "기관+외인 5일 쌍끌이 순매수 + 정배열 + 골든크로스/거래량 증가",
        "criteria": ["기관+외인 동반 순매수", "정배열(초기 이상)", "골든크로스 또는 거래량↑"],
        "color": "#16a34a",
    },
    "swing": {
        "title": "스윙매매",
        "desc": "볼린저 하단 근접 후 반등 + RSI 과매도 탈출 + 60일선 상방",
        "criteria": ["BB %B 0.2↓ → 반등", "RSI 과매도 탈출", "60일선 위", "양봉 반등"],
        "color": "#0891b2",
    },
}


def _render_strategy_desc(strat: dict):
    """전략 설명 카드."""
    info = _STRATEGY_DESC.get(strat["key"], {})
    color = info.get("color", "#4f46e5")
    desc = info.get("desc", "")
    criteria = info.get("criteria", [])

    criteria_html = " ".join([
        f'<span style="background:{color}15; color:{color}; padding:2px 8px; '
        f'border-radius:6px; font-size:0.72em; font-weight:600; margin:2px;">{c}</span>'
        for c in criteria
    ])

    st.markdown(
        f'<div style="background:#fff; border-left:4px solid {color}; '
        f'border-radius:10px; padding:12px 16px; margin-bottom:12px; '
        f'box-shadow:0 1px 3px rgba(0,0,0,0.05);">'
        f'<div style="font-size:0.88em; color:#475569; margin-bottom:6px;">{desc}</div>'
        f'<div style="display:flex; flex-wrap:wrap; gap:4px;">{criteria_html}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# 결과 카드 렌더링
# ═══════════════════════════════════════════════════════════════════════════

def _score_color(score: float) -> str:
    if score >= 80:
        return "#16a34a"
    elif score >= 60:
        return "#2563eb"
    elif score >= 40:
        return "#ea580c"
    return "#94a3b8"


def _render_result_cards(results: list, strategy_key: str = ""):
    """추천 종목 카드 그리드."""
    cols = st.columns(min(5, len(results)))

    for i, res in enumerate(results):
        with cols[i % len(cols)]:
            change = res["change"]
            price = res["price"]
            score = res["score"]
            chg_color = "#dc2626" if change > 0 else "#2563eb" if change < 0 else "#6b7280"
            sign = "+" if change > 0 else ""
            sc_color = _score_color(score)
            rank = i + 1

            rank_icons = {1: "🥇", 2: "🥈", 3: "🥉", 4: "4️⃣", 5: "5️⃣"}
            rank_icon = rank_icons.get(rank, f"#{rank}")

            tv_억 = res["volume"] / 1e8

            st.markdown(
                f'<div style="background:#fff; border-radius:14px; padding:14px; '
                f'border:1px solid #e2e8f0; box-shadow:0 2px 8px rgba(0,0,0,0.06); '
                f'margin-bottom:8px; min-height:220px;">'
                # 순위 + 점수
                f'<div style="display:flex; justify-content:space-between; align-items:center;">'
                f'<span style="font-size:1.2em;">{rank_icon}</span>'
                f'<span style="background:{sc_color}; color:#fff; padding:2px 8px; '
                f'border-radius:8px; font-size:0.75em; font-weight:700;">'
                f'{score:.0f}점</span>'
                f'</div>'
                # 종목명
                f'<div style="font-weight:700; font-size:0.95em; color:#1e293b; '
                f'margin:6px 0 2px;">{res["name"]}</div>'
                # 티커 + 업종
                f'<div style="font-size:0.7em; color:#94a3b8;">'
                f'{res["ticker"]} · {res["sector"]}</div>'
                # 가격
                f'<div style="font-size:1.15em; font-weight:800; color:{chg_color}; margin:6px 0;">'
                f'{price:,.0f}원'
                f'<span style="font-size:0.7em;"> {sign}{change:.1f}%</span>'
                f'</div>'
                # 거래대금
                f'<div style="font-size:0.72em; color:#64748b;">거래대금 {tv_억:,.0f}억</div>'
                # 추천 사유
                f'<div style="margin-top:6px; font-size:0.68em; color:#475569; '
                f'background:#f8fafc; padding:4px 6px; border-radius:6px; '
                f'line-height:1.4;">{res["reason"]}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
            wl_tickers = {e["ticker"] for e in get_watchlist()}
            in_wl = res["ticker"] in wl_tickers
            btn_label = "⭐ 관심 해제" if in_wl else "☆ 관심종목 추가"
            if st.button(btn_label, key=f"wl_strat_{strategy_key}_{i}_{res['ticker']}", use_container_width=True):
                if in_wl:
                    remove_from_watchlist(res["ticker"])
                else:
                    add_to_watchlist(
                        ticker=str(res["ticker"]),
                        name=str(res["name"]),
                        price=float(res["price"]),
                        sector=str(res["sector"]),
                        source="📋 전략추천",
                    )
                st.rerun()


def _render_result_table(results: list):
    """결과를 데이터 테이블로."""
    rows = []
    for res in results:
        rows.append({
            "순위": results.index(res) + 1,
            "종목명": res["name"],
            "티커": res["ticker"],
            "업종": res["sector"],
            "현재가": f'{res["price"]:,.0f}',
            "등락률": f'{res["change"]:+.1f}%',
            "점수": f'{res["score"]:.0f}',
            "추천사유": res["reason"],
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
