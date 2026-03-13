"""
🦵 무릎 아래 저평가 가치주 렌더링 컴포넌트

저평가 구간에서 반등 조짐이 보이는 종목을 카드/테이블로 표시.
"""

import streamlit as st
import pandas as pd
from typing import List, Dict

from logic_knee_stocks import screen_knee_stocks


# ═══════════════════════════════════════════════════════════════════════════
# 메인 렌더링
# ═══════════════════════════════════════════════════════════════════════════

def render_knee_stocks(daily_df: pd.DataFrame, date_str: str):
    """무릎 아래 저평가 가치주 영역 렌더링."""

    st.markdown("## 🦵 무릎 아래 · 저평가 가치주 발굴")
    st.caption(
        "PBR 저평가 + RSI 과매도 탈출 + 볼린저 하단 반등 + 이평 전환기 · "
        "바닥 다지기 끝나고 반등 초입 종목"
    )

    # 기준 설명 카드
    _render_criteria_card()

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    # 실행 버튼
    run_key = "knee_stock_run"
    result_key = "knee_stock_result"

    if st.button(
        "🔍 무릎 종목 스크리닝 실행",
        key=run_key,
        use_container_width=True,
        type="primary",
    ):
        with st.spinner("🦵 저평가 바닥 반등 종목 탐색 중... (PBR + 기술 지표 분석)"):
            results = screen_knee_stocks(daily_df, date_str, max_results=10)
        st.session_state[result_key] = results

    results = st.session_state.get(result_key, None)
    if results is None:
        st.info("위 버튼을 눌러 스크리닝을 실행하세요.")
        return

    if not results:
        st.warning("현재 조건에 맞는 무릎 종목이 없습니다.")
        return

    st.markdown(f"### 🏆 Top {len(results)} 무릎 종목")

    # 카드 렌더링
    _render_knee_cards(results)

    # 상세 테이블
    with st.expander("📊 상세 데이터 테이블", expanded=False):
        _render_knee_table(results)


# ═══════════════════════════════════════════════════════════════════════════
# 기준 설명 카드
# ═══════════════════════════════════════════════════════════════════════════

_CRITERIA = [
    ("PBR 0.6~1.0", "#7c3aed"),
    ("RSI 과매도탈출", "#dc2626"),
    ("볼린저 하단반등", "#2563eb"),
    ("배열 전환기", "#16a34a"),
    ("기관/외인 매집", "#ea580c"),
    ("ABCD C포인트", "#0891b2"),
]


def _render_criteria_card():
    badges = " ".join([
        f'<span style="background:{color}15; color:{color}; padding:3px 10px; '
        f'border-radius:8px; font-size:0.73em; font-weight:600; margin:2px;">{label}</span>'
        for label, color in _CRITERIA
    ])
    st.markdown(
        f'<div style="background:#fff; border-left:4px solid #7c3aed; '
        f'border-radius:10px; padding:12px 16px; margin-bottom:14px; '
        f'box-shadow:0 1px 3px rgba(0,0,0,0.05);">'
        f'<div style="font-size:0.86em; color:#475569; margin-bottom:8px;">'
        f'저평가 구간(PBR &lt; 1.0)에서 기관·외인이 조용히 매집하고, '
        f'기술적으로 바닥 반등 신호가 겹치는 종목을 찾습니다.</div>'
        f'<div style="display:flex; flex-wrap:wrap; gap:4px;">{badges}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# 결과 카드
# ═══════════════════════════════════════════════════════════════════════════

def _score_color(score: float) -> str:
    if score >= 80:
        return "#16a34a"
    elif score >= 60:
        return "#7c3aed"
    elif score >= 40:
        return "#ea580c"
    return "#94a3b8"


def _render_knee_cards(results: List[Dict]):
    """무릎 종목 카드 그리드 (2열)."""
    for row_start in range(0, len(results), 2):
        cols = st.columns(2)
        for j in range(2):
            idx = row_start + j
            if idx >= len(results):
                break
            res = results[idx]
            with cols[j]:
                _render_single_card(res, idx + 1)


def _render_single_card(res: Dict, rank: int):
    """개별 종목 카드."""
    change = res["change"]
    price = res["price"]
    score = res["score"]
    pbr = res["pbr"]
    rsi = res["rsi"]
    bb = res["bb_pctb"]

    chg_color = "#dc2626" if change > 0 else "#2563eb" if change < 0 else "#6b7280"
    sign = "+" if change > 0 else ""
    sc_color = _score_color(score)

    rank_icons = {1: "🥇", 2: "🥈", 3: "🥉"}
    rank_icon = rank_icons.get(rank, f"#{rank}")

    # 시그널 배지
    reason_badges = " ".join([
        f'<span style="background:#f1f5f9; color:#334155; padding:2px 6px; '
        f'border-radius:5px; font-size:0.65em; margin:1px;">{r}</span>'
        for r in res["reasons"]
    ])

    # 수급 정보
    inst = res["inst_5d"]
    frgn = res["frgn_5d"]
    inst_str = f'{inst/1e8:+,.0f}억' if abs(inst) >= 1e8 else f'{inst/1e6:+,.0f}백만'
    frgn_str = f'{frgn/1e8:+,.0f}억' if abs(frgn) >= 1e8 else f'{frgn/1e6:+,.0f}백만'

    gc_badge = ' <span style="color:#16a34a; font-size:0.7em;">✦골든크로스</span>' if res["golden_cross"] else ""
    abcd_badge = ' <span style="color:#0891b2; font-size:0.7em;">✦C포인트</span>' if res["abcd_c"] else ""

    st.markdown(
        f'<div style="background:#fff; border-radius:14px; padding:16px; '
        f'border:1px solid #e2e8f0; box-shadow:0 2px 8px rgba(0,0,0,0.06); '
        f'margin-bottom:10px;">'
        # 상단: 순위 + 점수
        f'<div style="display:flex; justify-content:space-between; align-items:center;">'
        f'<span style="font-size:1.2em;">{rank_icon}</span>'
        f'<span style="background:{sc_color}; color:#fff; padding:3px 10px; '
        f'border-radius:8px; font-size:0.78em; font-weight:700;">'
        f'{score:.0f}점 · {res["signal_count"]}시그널</span>'
        f'</div>'
        # 종목명
        f'<div style="font-weight:700; font-size:1.05em; color:#1e293b; '
        f'margin:8px 0 3px;">{res["name"]}{gc_badge}{abcd_badge}</div>'
        f'<div style="font-size:0.72em; color:#64748b;">{res["ticker"]} · {res["sector"]}</div>'
        # 가격 + 등락률
        f'<div style="margin:8px 0; display:flex; gap:12px; align-items:baseline;">'
        f'<span style="font-weight:700; font-size:1.1em;">{price:,.0f}원</span>'
        f'<span style="color:{chg_color}; font-weight:600; font-size:0.9em;">'
        f'{sign}{change:.2f}%</span>'
        f'</div>'
        # 핵심 지표 그리드
        f'<div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:6px; '
        f'margin:10px 0;">'
        f'<div style="text-align:center; background:#f8fafc; border-radius:8px; padding:6px;">'
        f'<div style="font-size:0.65em; color:#94a3b8;">PBR</div>'
        f'<div style="font-weight:700; color:#7c3aed; font-size:0.95em;">{pbr:.2f}</div></div>'
        f'<div style="text-align:center; background:#f8fafc; border-radius:8px; padding:6px;">'
        f'<div style="font-size:0.65em; color:#94a3b8;">RSI</div>'
        f'<div style="font-weight:700; color:#dc2626; font-size:0.95em;">{rsi:.0f}</div></div>'
        f'<div style="text-align:center; background:#f8fafc; border-radius:8px; padding:6px;">'
        f'<div style="font-size:0.65em; color:#94a3b8;">BB %B</div>'
        f'<div style="font-weight:700; color:#2563eb; font-size:0.95em;">{bb:.2f}</div></div>'
        f'</div>'
        # 수급
        f'<div style="font-size:0.72em; color:#475569; margin:6px 0;">'
        f'기관 5일: <b>{inst_str}</b> · 외인 5일: <b>{frgn_str}</b></div>'
        # 배열 상태
        f'<div style="font-size:0.72em; color:#475569; margin-bottom:8px;">'
        f'배열: <b>{res["alignment"]}</b></div>'
        # 시그널 배지
        f'<div style="display:flex; flex-wrap:wrap; gap:3px;">{reason_badges}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# 상세 테이블
# ═══════════════════════════════════════════════════════════════════════════

def _render_knee_table(results: List[Dict]):
    """상세 데이터 테이블."""
    rows = []
    for r in results:
        rows.append({
            "종목명": r["name"],
            "티커": r["ticker"],
            "현재가": f'{r["price"]:,.0f}',
            "등락률(%)": f'{r["change"]:+.2f}',
            "PBR": f'{r["pbr"]:.2f}',
            "RSI": f'{r["rsi"]:.0f}',
            "BB %B": f'{r["bb_pctb"]:.2f}',
            "배열": r["alignment"],
            "골든크로스": "✅" if r["golden_cross"] else "",
            "C포인트": "✅" if r["abcd_c"] else "",
            "기관5일(억)": f'{r["inst_5d"]/1e8:+,.0f}',
            "외인5일(억)": f'{r["frgn_5d"]/1e8:+,.0f}',
            "점수": r["score"],
            "시그널": ", ".join(r["reasons"]),
        })
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)
