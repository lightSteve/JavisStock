"""
📓 Trading Journal (매매 일지 · 복기)
- 스크린샷 기반 일일 복기 양식
  (매매 내역 테이블 + 복기 분석 + 리스크 체크 + 한줄 교훈)
- JSON 파일 영속 저장 (세션 종료 후에도 유지)
- 일별 목록 & 유형별 통계
"""

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime


from data.supabase_db import load_journal as _sb_load, save_journal as _sb_save


# ─────────────────────────────────────────────────────────────────────
# 사용자별 데이터 관리 (Supabase)
# ─────────────────────────────────────────────────────────────────────

def _get_username() -> str:
    return st.session_state.get("username", "default")


def _journal_key() -> str:
    return f"trading_journal_{_get_username()}"


def _load_journal() -> list:
    """Supabase에서 매매 기록을 로드."""
    return _sb_load(_get_username())


def _save_journal(entries: list):
    """매매 기록을 Supabase에 저장."""
    _sb_save(_get_username(), entries)


def _get_entries() -> list:
    """세션 + 파일 동기화. 세션에 없으면 파일에서 로드."""
    key = _journal_key()
    if key not in st.session_state:
        st.session_state[key] = _load_journal()
    return st.session_state[key]


def _add_entry(entry: dict):
    """기록 추가 후 파일 저장."""
    key = _journal_key()
    entries = _get_entries()
    entries.append(entry)
    st.session_state[key] = entries
    _save_journal(entries)


def _delete_entry(timestamp: str):
    """타임스탬프 기준 기록 삭제 후 파일 저장."""
    key = _journal_key()
    entries = _get_entries()
    entries = [e for e in entries if e.get("timestamp") != timestamp]
    st.session_state[key] = entries
    _save_journal(entries)


def render_trading_journal(daily_df: pd.DataFrame, date_str: str):
    """매매 일지 & 복기 렌더링."""
    st.markdown("## 📓 매매 일지 & 복기")
    st.caption("일별 매매 내역 · 복기 분석 · 리스크 체크 · 한줄 교훈")

    username = _get_username()
    if username == "default":
        st.warning("⚠️ 사이드바에서 **닉네임**을 입력하면 개인별로 매매일지가 저장됩니다.")
    else:
        st.caption(f"👤 **{username}** 님의 매매일지")

    _get_entries()

    tab_add, tab_log, tab_stats = st.tabs(["✏️ 오늘의 복기", "📋 일지 목록", "📊 통계"])

    with tab_add:
        _render_daily_review(daily_df, date_str)

    with tab_log:
        _render_journal_list()

    with tab_stats:
        _render_journal_stats()


# ═══════════════════════════════════════════════════════════════════════════
# ✏️ 오늘의 복기 (스크린샷 양식 기반)
# ═══════════════════════════════════════════════════════════════════════════

def _render_daily_review(daily_df: pd.DataFrame, date_str: str):
    """스크린샷 양식 기반 일일 복기 폼."""

    # ── 헤더: 날짜 + 요약 정보 ──
    dt = datetime.strptime(date_str, "%Y%m%d")
    date_display = dt.strftime("%Y.%m.%d")

    st.markdown(
        f'<div style="background:#1e293b; color:#e2e8f0; border-radius:12px; '
        f'padding:16px 20px; margin-bottom:16px;">'
        f'<div style="font-size:1.05em; font-weight:700;">📅 날짜: {date_display}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # 총 거래 건수 · 실현 수익률 · 계좌 잔고
    c1, c2, c3 = st.columns(3)
    with c1:
        trade_count = st.number_input("📊 총 거래 건수", min_value=0, step=1, value=0, key="rv_count")
    with c2:
        realized_pnl = st.number_input("💰 실현 수익률 (%)", step=0.1, value=0.0, format="%.1f", key="rv_pnl")
    with c3:
        balance = st.number_input("🏦 계좌 잔고 (만원)", min_value=0, step=10, value=0, key="rv_balance")

    st.markdown("---")

    # ═══ 매매 내역 섹션 ═══
    st.markdown(
        '<div style="font-weight:700; font-size:1.0em; color:#1e293b; '
        'margin-bottom:10px;">📋 매매 내역</div>',
        unsafe_allow_html=True,
    )

    # 동적 매매 내역 행 관리
    if "rv_trades" not in st.session_state:
        st.session_state["rv_trades"] = [_empty_trade()]

    trades = st.session_state["rv_trades"]

    # 테이블 헤더
    st.markdown(
        '<div style="display:grid; grid-template-columns:1.2fr 0.8fr 1fr 0.8fr 1fr 0.8fr 0.7fr; '
        'gap:6px; padding:6px 8px; background:#f1f5f9; border-radius:8px; '
        'font-size:0.72em; font-weight:600; color:#64748b; margin-bottom:4px;">'
        '<div>종목</div><div>매수/매도</div><div>가격</div><div>수량</div>'
        '<div>금액</div><div>수익률</div><div>손절/익절</div></div>',
        unsafe_allow_html=True,
    )

    for i, t in enumerate(trades):
        cols = st.columns([1.2, 0.8, 1, 0.8, 1, 0.8, 0.7])
        with cols[0]:
            trades[i]["name"] = st.text_input("종목", value=t["name"], key=f"t_name_{i}",
                                               label_visibility="collapsed", placeholder="종목명")
        with cols[1]:
            trades[i]["direction"] = st.selectbox("방향", ["매수", "매도"], key=f"t_dir_{i}",
                                                   index=0 if t["direction"] == "매수" else 1,
                                                   label_visibility="collapsed")
        with cols[2]:
            trades[i]["price"] = st.number_input("가격", min_value=0, step=100, value=t["price"],
                                                  key=f"t_price_{i}", label_visibility="collapsed")
        with cols[3]:
            trades[i]["qty"] = st.number_input("수량", min_value=0, step=1, value=t["qty"],
                                                key=f"t_qty_{i}", label_visibility="collapsed")
        with cols[4]:
            amt = trades[i]["price"] * trades[i]["qty"]
            st.markdown(f'<div style="padding:8px 0; font-size:0.9em;">{amt:,.0f}원</div>',
                        unsafe_allow_html=True)
        with cols[5]:
            trades[i]["pnl_pct"] = st.number_input("수익률%", step=0.1, value=t["pnl_pct"],
                                                    format="%.1f", key=f"t_pnl_{i}",
                                                    label_visibility="collapsed")
        with cols[6]:
            trades[i]["cut"] = st.selectbox("손/익", ["–", "손절", "익절"],
                                             key=f"t_cut_{i}",
                                             index=["–", "손절", "익절"].index(t["cut"]),
                                             label_visibility="collapsed")

    bc1, bc2 = st.columns(2)
    with bc1:
        if st.button("➕ 매매 행 추가", key="add_trade_row", use_container_width=True):
            st.session_state["rv_trades"].append(_empty_trade())
            st.rerun()
    with bc2:
        if len(trades) > 1 and st.button("➖ 마지막 행 삭제", key="del_trade_row", use_container_width=True):
            st.session_state["rv_trades"].pop()
            st.rerun()

    st.markdown("---")

    # ═══ 복기 분석 섹션 ═══
    st.markdown(
        '<div style="font-weight:700; font-size:1.0em; color:#1e293b; '
        'margin-bottom:10px;">🔍 복기 분석</div>',
        unsafe_allow_html=True,
    )

    entry_reason = st.text_area(
        "🎯 진입 이유", height=60, key="rv_entry",
        placeholder="차트 패턴/볼륨/뉴스 등 구체적으로",
    )
    exit_reason = st.text_area(
        "🏁 청산 이유", height=60, key="rv_exit",
        placeholder="손절 기준/목표 도달 등",
    )
    good_point = st.text_area(
        "🧠 잘한 점", height=50, key="rv_good",
        placeholder="1-2문장",
    )
    bad_point = st.text_area(
        "❌ 실수/개선점", height=50, key="rv_bad",
        placeholder="1-2문장, 다음에 고칠 행동",
    )
    emotion = st.text_area(
        "💀 감정 상태", height=50, key="rv_emotion",
        placeholder="공포/탐욕/침착 등 솔직히 기록",
    )

    st.markdown("---")

    # ═══ 리스크 체크 섹션 ═══
    st.markdown(
        '<div style="font-weight:700; font-size:1.0em; color:#1e293b; '
        'margin-bottom:10px;">🛡️ 리스크 체크</div>',
        unsafe_allow_html=True,
    )

    rc1, rc2 = st.columns(2)
    with rc1:
        stoploss_followed = st.selectbox(
            "손절 원칙 준수 여부", ["O", "X"], key="rv_sl", index=0
        )
        stoploss_pct = st.number_input(
            "손절 기준 (%)", step=0.5, value=2.0, format="%.1f", key="rv_sl_pct"
        )
    with rc2:
        position_pct = st.number_input(
            "포지션 비중 (%)", min_value=0.0, max_value=100.0,
            step=5.0, value=20.0, format="%.0f", key="rv_pos"
        )
        cash_pct = st.number_input(
            "현금 비중 (%)", min_value=0.0, max_value=100.0,
            step=5.0, value=50.0, format="%.0f", key="rv_cash"
        )

    st.markdown("---")

    # ═══ 오늘 한줄 교훈 ═══
    lesson = st.text_input(
        "✏️ 오늘 한줄 교훈", key="rv_lesson",
        placeholder="반드시 쓰기 – 다음 날 첫 행동으로 연결",
    )

    st.markdown("")

    # ═══ 저장 ═══
    if st.button("💾 오늘의 복기 저장", type="primary", use_container_width=True, key="rv_save"):
        # 매매 내역 중 종목명이 있는 것만
        valid_trades = [t for t in trades if t["name"].strip()]
        if not valid_trades and not lesson.strip():
            st.warning("매매 내역 또는 한줄 교훈을 작성해주세요.")
            return

        entry = {
            "date": date_str,
            "timestamp": datetime.now().isoformat(),
            "trade_count": trade_count,
            "realized_pnl": realized_pnl,
            "balance": balance,
            "trades": valid_trades,
            "entry_reason": entry_reason,
            "exit_reason": exit_reason,
            "good_point": good_point,
            "bad_point": bad_point,
            "emotion": emotion,
            "stoploss_followed": stoploss_followed,
            "stoploss_pct": stoploss_pct,
            "position_pct": position_pct,
            "cash_pct": cash_pct,
            "lesson": lesson,
            # 하위 호환용 필드
            "ticker": valid_trades[0]["name"] if valid_trades else "복기",
            "trade_type": "복기",
            "trade_type_label": "복기",
            "direction": valid_trades[0]["direction"] if valid_trades else "–",
            "price": valid_trades[0]["price"] if valid_trades else 0,
            "quantity": valid_trades[0]["qty"] if valid_trades else 0,
            "result": _infer_result(valid_trades),
            "trade_memo": lesson,
            "market_memo": entry_reason,
        }
        _add_entry(entry)
        st.session_state["rv_trades"] = [_empty_trade()]
        st.success("✅ 오늘의 복기가 저장되었습니다!")
        st.rerun()


def _empty_trade() -> dict:
    return {"name": "", "direction": "매수", "price": 0, "qty": 0, "pnl_pct": 0.0, "cut": "–"}


def _infer_result(trades: list) -> str:
    if not trades:
        return "진행중"
    cuts = [t["cut"] for t in trades]
    if "익절" in cuts:
        return "수익실현"
    if "손절" in cuts:
        return "손절"
    pnls = [t["pnl_pct"] for t in trades]
    avg = sum(pnls) / len(pnls) if pnls else 0
    if avg > 0:
        return "수익실현"
    elif avg < 0:
        return "손절"
    return "본전"


# ═══════════════════════════════════════════════════════════════════════════
# 📋 일지 목록
# ═══════════════════════════════════════════════════════════════════════════

def _render_journal_list():
    """매매 일지 리스트 (신양식 + 구양식 호환)."""
    st.markdown("### 📋 매매 일지")

    entries = _get_entries()
    if not entries:
        st.info("등록된 매매 기록이 없습니다.")
        return

    sorted_entries = sorted(entries, key=lambda x: x.get("timestamp", ""), reverse=True)

    for i, entry in enumerate(sorted_entries):
        is_new_format = "trades" in entry

        result = entry.get("result", "진행중")
        result_map = {
            "수익실현": ("🟢", "#16a34a"),
            "손절": ("🔴", "#dc2626"),
            "본전": ("⚪", "#6b7280"),
            "진행중": ("🔵", "#3b82f6"),
        }
        icon, color = result_map.get(result, ("⚪", "#6b7280"))

        _card_col, _del_col = st.columns([12, 1])
        with _card_col:
            if is_new_format:
                _render_new_format_card(entry, icon, color)
            else:
                _render_legacy_card(entry, icon, color)
        with _del_col:
            if st.button("🗑", key=f"del_journal_{i}", help="이 기록 삭제"):
                _delete_entry(entry.get("timestamp", ""))
                st.rerun()


def _render_new_format_card(entry: dict, icon: str, color: str):
    """신양식 카드 (trades + 복기 + 리스크)."""
    dt = entry.get("date", "")
    lesson = entry.get("lesson", "")
    trades = entry.get("trades", [])
    rpnl = entry.get("realized_pnl", 0)

    # 매매 요약
    trade_lines = ""
    for t in trades:
        amt = t["price"] * t["qty"]
        cut_badge = ""
        if t.get("cut") in ("손절", "익절"):
            cut_color = "#dc2626" if t["cut"] == "손절" else "#16a34a"
            cut_badge = (f'<span style="color:{cut_color}; font-weight:600; '
                         f'font-size:0.72em; margin-left:4px;">{t["cut"]}</span>')
        trade_lines += (
            f'<div style="font-size:0.78em; color:#334155; margin:2px 0;">'
            f'<b>{t["name"]}</b> {t["direction"]} '
            f'{t["price"]:,.0f}원 × {t["qty"]}주 = {amt:,.0f}원 '
            f'<span style="color:{"#dc2626" if t["pnl_pct"] < 0 else "#16a34a"};">'
            f'{t["pnl_pct"]:+.1f}%</span>{cut_badge}'
            f'</div>'
        )

    # 복기 요약
    review_parts = []
    if entry.get("entry_reason"):
        review_parts.append(f'🎯 {entry["entry_reason"]}')
    if entry.get("good_point"):
        review_parts.append(f'🧠 {entry["good_point"]}')
    if entry.get("bad_point"):
        review_parts.append(f'❌ {entry["bad_point"]}')
    review_html = ""
    if review_parts:
        review_html = '<div style="font-size:0.73em; color:#475569; margin-top:6px;">'
        review_html += "<br>".join(review_parts) + '</div>'

    lesson_html = ""
    if lesson:
        lesson_html = (
            f'<div style="font-size:0.78em; color:#7c3aed; font-weight:600; '
            f'margin-top:6px; border-top:1px solid #e2e8f0; padding-top:6px;">'
            f'✏️ {lesson}</div>'
        )

    sl = entry.get("stoploss_followed", "")
    sl_badge = ""
    if sl:
        sl_color = "#16a34a" if sl == "O" else "#dc2626"
        sl_badge = (f'<span style="background:{sl_color}15; color:{sl_color}; '
                    f'padding:1px 6px; border-radius:4px; font-size:0.68em; '
                    f'margin-left:8px;">손절준수:{sl}</span>')

    rpnl_color = "#16a34a" if rpnl > 0 else "#dc2626" if rpnl < 0 else "#6b7280"

    st.markdown(
        f'<div style="background:#fff; border-radius:12px; padding:12px 16px; '
        f'border-left:4px solid {color}; margin-bottom:8px; '
        f'box-shadow:0 1px 3px rgba(0,0,0,0.05);">'
        f'<div style="display:flex; justify-content:space-between; align-items:center;">'
        f'<div>'
        f'<span style="font-weight:700; font-size:0.95em;">{icon} {dt}</span>'
        f'<span style="font-size:0.73em; color:{rpnl_color}; font-weight:600; '
        f'margin-left:8px;">수익률 {rpnl:+.1f}%</span>{sl_badge}'
        f'</div>'
        f'<span style="color:{color}; font-weight:600; font-size:0.82em;">{entry.get("result","")}</span>'
        f'</div>'
        f'<div style="margin-top:6px;">{trade_lines}</div>'
        f'{review_html}{lesson_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


def _render_legacy_card(entry: dict, icon: str, color: str):
    """구양식 카드 (하위 호환)."""
    _memo = entry.get("trade_memo", "") or ""
    _memo_html = (f'<div style="font-size:0.78em;color:#475569;margin-top:4px;">'
                  f'{_memo}</div>' if _memo else "")
    st.markdown(
        f'<div style="background:#fff; border-radius:10px; padding:10px 14px; '
        f'border-left:4px solid {color}; margin-bottom:6px;">'
        f'<div style="display:flex; justify-content:space-between; align-items:center;">'
        f'<div>'
        f'<span style="font-weight:700;">{icon} {entry["ticker"]}</span>'
        f'<span style="background:#e2e8f0; padding:1px 6px; border-radius:4px; '
        f'font-size:0.68em; margin-left:6px;">{entry.get("trade_type_label","")}</span>'
        f'<span style="color:#94a3b8; font-size:0.75em; margin-left:6px;">'
        f'{entry.get("direction","")}</span>'
        f'</div>'
        f'<span style="color:{color}; font-weight:600; font-size:0.85em;">'
        f'{entry.get("result","")}</span>'
        f'</div>'
        f'<div style="font-size:0.78em; color:#64748b; margin-top:4px;">'
        f'{entry.get("date","")} · {entry.get("price",0):,.0f}원 × '
        f'{entry.get("quantity",0)}주</div>'
        f'{_memo_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


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
