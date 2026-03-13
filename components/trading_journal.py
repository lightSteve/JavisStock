"""
📓 Trading Journal (매매 일지 · 복기)
- 일별 시장 요약 로그
- 매매 기록 (유형 A-E 분류)
- 유형별 매매 횟수 · 승률 통계
- JSON 파일 영속 저장 (세션 종료 후에도 유지)
"""

import json
import os
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime


_JOURNAL_KEY = "trading_journal"
_JOURNAL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "journal_data")
_JOURNAL_FILE = os.path.join(_JOURNAL_DIR, "trading_journal.json")


# ─────────────────────────────────────────────────────────────────────
# 영속 저장/로드
# ─────────────────────────────────────────────────────────────────────

def _ensure_dir():
    os.makedirs(_JOURNAL_DIR, exist_ok=True)


def _load_journal() -> list:
    """JSON 파일에서 매매 기록을 로드."""
    if os.path.exists(_JOURNAL_FILE):
        try:
            with open(_JOURNAL_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
        except (json.JSONDecodeError, IOError):
            pass
    return []


def _save_journal(entries: list):
    """매매 기록을 JSON 파일에 저장."""
    _ensure_dir()
    with open(_JOURNAL_FILE, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)


def _get_entries() -> list:
    """세션 + 파일 동기화. 세션에 없으면 파일에서 로드."""
    if _JOURNAL_KEY not in st.session_state:
        st.session_state[_JOURNAL_KEY] = _load_journal()
    return st.session_state[_JOURNAL_KEY]


def _add_entry(entry: dict):
    """기록 추가 후 파일 저장."""
    entries = _get_entries()
    entries.append(entry)
    st.session_state[_JOURNAL_KEY] = entries
    _save_journal(entries)


def _delete_entry(timestamp: str):
    """타임스탬프 기준 기록 삭제 후 파일 저장."""
    entries = _get_entries()
    entries = [e for e in entries if e.get("timestamp") != timestamp]
    st.session_state[_JOURNAL_KEY] = entries
    _save_journal(entries)


def render_trading_journal(daily_df: pd.DataFrame, date_str: str):
    """매매 일지 & 복기 렌더링."""
    st.markdown("## 📓 매매 일지 & 복기")
    st.caption("일별 시장 요약 · 매매 기록 · 유형별 통계")

    # 파일에서 로드 (최초 1회)
    _get_entries()

    tab_log, tab_add, tab_stats = st.tabs(["📋 일지 목록", "➕ 기록 추가", "📊 통계"])

    with tab_add:
        _render_add_entry(daily_df, date_str)

    with tab_log:
        _render_journal_list()

    with tab_stats:
        _render_journal_stats()


# ─────────────────────────────────────────────────────────────────────
def _render_add_entry(daily_df: pd.DataFrame, date_str: str):
    """매매 기록 추가 폼."""
    st.markdown("### ➕ 매매 기록 추가")

    # 시장 요약 자동 채움
    if not daily_df.empty:
        up_ratio = (daily_df["등락률"] > 0).mean() * 100 if "등락률" in daily_df.columns else 0
        avg_chg = daily_df["등락률"].mean() if "등락률" in daily_df.columns else 0
        total_tv = daily_df["거래대금"].sum() / 1e12 if "거래대금" in daily_df.columns else 0

        st.markdown(
            f'<div style="background:#f8fafc; border-radius:10px; padding:10px; '
            f'margin-bottom:12px; font-size:0.85em;">'
            f'📅 {date_str} · 상승비율 {up_ratio:.0f}% · 평균등락 {avg_chg:+.2f}% · '
            f'거래대금 {total_tv:.1f}조</div>',
            unsafe_allow_html=True,
        )

    with st.form("add_journal_entry", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            ticker = st.text_input("종목코드", placeholder="005930")
            trade_type = st.selectbox("매매 유형", ["A:테마추격", "B:뉴스스파이크", "C:돌파매매", "D:바이오회복", "E:스윙"])
            direction = st.selectbox("방향", ["매수", "매도"])
        with c2:
            price = st.number_input("체결가 (원)", min_value=0, step=100)
            quantity = st.number_input("수량", min_value=0, step=1, value=1)
            result = st.selectbox("결과", ["진행중", "수익실현", "손절", "본전"])

        market_memo = st.text_area("시장 요약 / 메모", height=60, placeholder="당일 시장 분위기, 주요 이슈...")
        trade_memo = st.text_area("매매 복기", height=60, placeholder="진입 근거, 느낀 점, 개선할 점...")

        submitted = st.form_submit_button("기록 추가", type="primary", use_container_width=True)
        if submitted and ticker:
            entry = {
                "date": date_str,
                "timestamp": datetime.now().isoformat(),
                "ticker": ticker.strip(),
                "trade_type": trade_type.split(":")[0],
                "trade_type_label": trade_type,
                "direction": direction,
                "price": float(price),
                "quantity": int(quantity),
                "result": result,
                "market_memo": market_memo,
                "trade_memo": trade_memo,
            }
            _add_entry(entry)
            st.success(f"✅ 매매 기록이 추가되었습니다. ({ticker} {direction})")


# ─────────────────────────────────────────────────────────────────────
def _render_journal_list():
    """매매 일지 리스트."""
    st.markdown("### 📋 매매 일지")

    entries = _get_entries()
    if not entries:
        st.info("등록된 매매 기록이 없습니다.")
        return

    # 최신순 정렬
    sorted_entries = sorted(entries, key=lambda x: x.get("timestamp", ""), reverse=True)

    for i, entry in enumerate(sorted_entries):
        result_map = {
            "수익실현": ("🟢", "#16a34a"),
            "손절": ("🔴", "#dc2626"),
            "본전": ("⚪", "#6b7280"),
            "진행중": ("🔵", "#3b82f6"),
        }
        icon, color = result_map.get(entry["result"], ("⚪", "#6b7280"))

        _card_col, _del_col = st.columns([12, 1])
        with _card_col:
            _memo = entry.get("trade_memo", "") or ""
            _memo_html = f'<div style="font-size:0.78em;color:#475569;margin-top:4px;">{_memo}</div>' if _memo else ""
            st.markdown(
                f'<div style="background:#fff; border-radius:10px; padding:10px 14px; '
                f'border-left:4px solid {color}; margin-bottom:6px;">'
                f'<div style="display:flex; justify-content:space-between; align-items:center;">'
                f'<div>'
                f'<span style="font-weight:700;">{icon} {entry["ticker"]}</span>'
                f'<span style="background:#e2e8f0; padding:1px 6px; border-radius:4px; '
                f'font-size:0.68em; margin-left:6px;">{entry["trade_type_label"]}</span>'
                f'<span style="color:#94a3b8; font-size:0.75em; margin-left:6px;">{entry["direction"]}</span>'
                f'</div>'
                f'<span style="color:{color}; font-weight:600; font-size:0.85em;">{entry["result"]}</span>'
                f'</div>'
                f'<div style="font-size:0.78em; color:#64748b; margin-top:4px;">'
                f'{entry["date"]} · {entry["price"]:,.0f}원 × {entry["quantity"]}주'
                f'</div>'
                f'{_memo_html}'
                f'</div>',
                unsafe_allow_html=True,
            )
        with _del_col:
            if st.button("🗑", key=f"del_journal_{i}", help="이 기록 삭제"):
                _delete_entry(entry.get("timestamp", ""))
                st.rerun()


# ─────────────────────────────────────────────────────────────────────
def _render_journal_stats():
    """매매 통계."""
    st.markdown("### 📊 매매 유형별 통계")

    entries = _get_entries()
    if not entries:
        st.info("통계를 표시할 매매 기록이 없습니다.")
        return

    df = pd.DataFrame(entries)

    # 유형별 집계
    type_stats = df.groupby("trade_type_label").agg(
        매매횟수=("ticker", "count"),
        수익실현=("result", lambda x: (x == "수익실현").sum()),
        손절=("result", lambda x: (x == "손절").sum()),
    ).reset_index()
    type_stats["승률"] = (
        type_stats["수익실현"] / (type_stats["수익실현"] + type_stats["손절"]).replace(0, 1) * 100
    ).round(1)

    st.dataframe(type_stats, use_container_width=True, hide_index=True)

    # 유형별 매매 횟수 차트
    fig = go.Figure(go.Bar(
        x=type_stats["trade_type_label"],
        y=type_stats["매매횟수"],
        marker_color=["#4f46e5", "#dc2626", "#f59e0b", "#16a34a", "#7c3aed"][:len(type_stats)],
        text=type_stats["매매횟수"],
        textposition="outside",
    ))
    fig.update_layout(
        title="매매 유형별 횟수",
        height=300,
        template="plotly_white",
        margin=dict(l=10, r=10, t=40, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

    # 결과 분포 파이
    result_counts = df["result"].value_counts()
    colors_map = {"수익실현": "#16a34a", "손절": "#dc2626", "본전": "#6b7280", "진행중": "#3b82f6"}
    fig2 = go.Figure(go.Pie(
        labels=result_counts.index.tolist(),
        values=result_counts.values.tolist(),
        hole=0.4,
        marker_colors=[colors_map.get(r, "#94a3b8") for r in result_counts.index],
    ))
    fig2.update_layout(
        title="매매 결과 분포",
        height=280,
        margin=dict(l=10, r=10, t=40, b=10),
    )
    st.plotly_chart(fig2, use_container_width=True)
