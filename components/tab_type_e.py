"""
⚡ Type E: 단기 스윙 + 포지션 관리
- 보유 포지션 입력/관리
- 손절/목표가 시각화
- 포트폴리오 리스크 대시보드
"""

import json
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from logic_patterns import calc_position_risk, build_portfolio_summary


_SESSION_KEY = "positions_type_e"


def render_tab_type_e(daily_df: pd.DataFrame, date_str: str):
    """Type E 탭 렌더링: 단기 스윙 + 포지션 관리."""
    st.markdown("## ⚡ Type E: 단기 스윙 · 포지션 관리")
    st.caption("포지션 등록 · 손절/목표가 · 리스크 대시보드")

    if _SESSION_KEY not in st.session_state:
        st.session_state[_SESSION_KEY] = []

    tab_add, tab_dash = st.tabs(["➕ 포지션 등록", "📊 포트폴리오"])

    with tab_add:
        _render_add_position(daily_df)

    with tab_dash:
        _render_portfolio_dashboard(daily_df)


# ─────────────────────────────────────────────────────────────────────
def _render_add_position(daily_df: pd.DataFrame):
    """포지션 등록 폼."""
    st.markdown("### ➕ 포지션 등록")

    with st.form("add_position_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            ticker_input = st.text_input("종목코드", placeholder="예: 005930")
            entry_price = st.number_input("매수가 (원)", min_value=0, step=100)
            quantity = st.number_input("수량", min_value=0, step=1, value=1)
        with c2:
            trade_type = st.selectbox("매매유형", ["A:테마추격", "B:뉴스스파이크", "C:돌파매매", "D:바이오회복", "E:스윙"])
            stop_loss = st.number_input("손절 기준 (%)", value=-3.0, step=0.5, max_value=0.0)
            target = st.number_input("목표 수익 (%)", value=10.0, step=1.0, min_value=0.0)

        memo = st.text_area("메모 / 매매 근거", height=60, placeholder="매수 이유, 특이사항...")

        submitted = st.form_submit_button("등록", type="primary", use_container_width=True)
        if submitted and ticker_input and entry_price > 0 and quantity > 0:
            pos = {
                "ticker": ticker_input.strip(),
                "entry_price": float(entry_price),
                "quantity": int(quantity),
                "trade_type": trade_type.split(":")[0],
                "stop_loss_pct": float(stop_loss),
                "target_pct": float(target),
                "memo": memo,
                "date": date_str if "date_str" in dir() else "",
            }
            st.session_state[_SESSION_KEY].append(pos)
            st.success(f"✅ {ticker_input} 포지션이 등록되었습니다.")

    # 등록된 포지션 목록
    positions = st.session_state[_SESSION_KEY]
    if positions:
        st.markdown("#### 📋 등록된 포지션")
        for i, pos in enumerate(positions):
            c1, c2, c3 = st.columns([3, 1, 1])
            with c1:
                st.markdown(
                    f"**{pos['ticker']}** · {pos['trade_type']} · "
                    f"매수가 {pos['entry_price']:,.0f}원 × {pos['quantity']}주"
                )
            with c2:
                st.markdown(f"손절 {pos['stop_loss_pct']}% / 목표 {pos['target_pct']}%")
            with c3:
                if st.button("❌", key=f"del_pos_{i}"):
                    st.session_state[_SESSION_KEY].pop(i)
                    st.rerun()
    else:
        st.info("등록된 포지션이 없습니다. 위 폼에서 포지션을 추가하세요.")


# ─────────────────────────────────────────────────────────────────────
def _render_portfolio_dashboard(daily_df: pd.DataFrame):
    """포트폴리오 리스크 대시보드."""
    st.markdown("### 📊 포트폴리오 대시보드")

    positions = st.session_state.get(_SESSION_KEY, [])
    if not positions:
        st.info("등록된 포지션이 없습니다.")
        return

    summary = build_portfolio_summary(positions, daily_df)

    # ── 전체 요약 메트릭 ──
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("총 투자금", f"{summary['total_cost']:,.0f}원")
    with c2:
        st.metric("총 평가금", f"{summary['total_value']:,.0f}원")
    with c3:
        pnl_color = "normal" if summary["total_pnl"] >= 0 else "inverse"
        st.metric("총 손익", f"{summary['total_pnl']:+,.0f}원",
                  f"{summary['total_pnl_pct']:+.1f}%", delta_color=pnl_color)
    with c4:
        st.metric("승/패", f"{summary['winning']}W / {summary['losing']}L")

    # ── 손절/목표 도달 경고 ──
    if summary["at_stop"] > 0:
        st.error(f"🔴 손절 기준 도달 종목: {summary['at_stop']}개 — 즉시 확인 필요!")
    if summary["at_target"] > 0:
        st.success(f"🎯 목표 도달 종목: {summary['at_target']}개 — 수익 실현 고려")

    # ── 개별 종목 카드 ──
    st.markdown("#### 종목별 현황")
    for pos_detail in summary["positions"]:
        ticker = pos_detail["ticker"]
        name = pos_detail.get("종목명", ticker)
        pnl_pct = pos_detail["pnl_pct"]
        status = pos_detail["status"]

        status_map = {
            "손절": ("🔴", "#dc2626"),
            "목표도달": ("🎯", "#16a34a"),
            "수익중": ("🟢", "#22c55e"),
            "손실중": ("🔵", "#3b82f6"),
        }
        icon, color = status_map.get(status, ("⚪", "#6b7280"))

        entry = pos_detail["entry_price"]
        current = pos_detail["current_price"]
        stop_p = pos_detail["stop_price"]
        target_p = pos_detail["target_price"]
        rr = pos_detail["risk_reward"]

        st.markdown(
            f'<div style="background:#fff; border-radius:10px; padding:12px 14px; '
            f'border-left:4px solid {color}; margin-bottom:6px; '
            f'box-shadow:0 1px 3px rgba(0,0,0,0.05);">'
            f'<div style="display:flex; justify-content:space-between; align-items:center;">'
            f'<div>'
            f'<span style="font-weight:700; font-size:0.95em;">{icon} {name}</span>'
            f'<span style="color:#94a3b8; font-size:0.75em; margin-left:6px;">{ticker}</span>'
            f'</div>'
            f'<span style="color:{color}; font-weight:700; font-size:1.1em;">{pnl_pct:+.1f}% {status}</span>'
            f'</div>'
            f'<div style="display:flex; gap:16px; margin-top:6px; font-size:0.78em; color:#64748b;">'
            f'<span>매수 {entry:,.0f}</span>'
            f'<span>현재 {current:,.0f}</span>'
            f'<span style="color:#dc2626;">손절 {stop_p:,.0f}</span>'
            f'<span style="color:#16a34a;">목표 {target_p:,.0f}</span>'
            f'<span>R:R {rr:.1f}</span>'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # ── 유형별 분포 차트 ──
    type_counts = {}
    for p in summary["positions"]:
        t = p.get("trade_type", "기타")
        type_counts[t] = type_counts.get(t, 0) + 1

    if type_counts:
        fig = go.Figure(go.Pie(
            labels=list(type_counts.keys()),
            values=list(type_counts.values()),
            hole=0.4,
            marker_colors=["#4f46e5", "#dc2626", "#f59e0b", "#16a34a", "#7c3aed"],
        ))
        fig.update_layout(
            title="매매 유형별 포지션 분포",
            height=280,
            margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)
