"""
관심종목 컴포넌트

- 발굴 탭에서 체크박스/버튼으로 관심종목 추가
- 추가 당시 현재가 기록 → 가상 시세차익 계산 및 표시
- Supabase 저장 / session_state 캐시
"""

from __future__ import annotations

import datetime
from typing import Optional

import pandas as pd
import streamlit as st

from data.supabase_db import load_watchlist as _sb_load, save_watchlist as _sb_save


# ─────────────────────────────────────────────────────────────────────
# 내부 헬퍼
# ─────────────────────────────────────────────────────────────────────

def _get_username() -> str:
    return st.session_state.get("username", "default")


def _session_key() -> str:
    return f"watchlist_{_get_username()}"


def _load_watchlist() -> list:
    try:
        return _sb_load(_get_username())
    except Exception:
        return []


def _save_watchlist(entries: list):
    try:
        _sb_save(_get_username(), entries)
    except Exception:
        pass


def get_watchlist() -> list:
    """현재 관심종목 리스트 반환 (session_state 캐시 우선)."""
    key = _session_key()
    if key not in st.session_state:
        st.session_state[key] = _load_watchlist()
    return st.session_state[key]


def add_to_watchlist(
    ticker: str,
    name: str,
    price: float,
    sector: str = "",
    market: str = "",
    source: str = "",
) -> bool:
    """관심종목 추가. 이미 존재하면 False 반환. 로그인 안 된 경우 False 반환."""
    if _get_username() == "default":
        st.toast("⚠️ 관심종목을 추가하려면 먼저 사이드바에서 로그인하세요.", icon="🔐")
        return False
    entries = get_watchlist()
    if any(e["ticker"] == ticker for e in entries):
        return False
    entry = {
        "ticker": ticker,
        "name": name,
        "added_price": float(price),
        "added_date": datetime.date.today().isoformat(),
        "sector": sector,
        "market": market,
        "source": source,
    }
    entries = [entry] + entries  # 최신 항목이 위에 오도록
    st.session_state[_session_key()] = entries
    _save_watchlist(entries)
    return True


def remove_from_watchlist(ticker: str) -> bool:
    """관심종목 제거. 없으면 False 반환."""
    entries = get_watchlist()
    new_entries = [e for e in entries if e["ticker"] != ticker]
    if len(new_entries) == len(entries):
        return False
    st.session_state[_session_key()] = new_entries
    _save_watchlist(new_entries)
    return True


# ─────────────────────────────────────────────────────────────────────
# 카드 렌더링
# ─────────────────────────────────────────────────────────────────────

def _source_badge_html(source: str) -> str:
    """출처(source) 배지 HTML 반환."""
    if not source:
        return ""
    if "AI Top3" in source or "스마트" in source:
        color = "#1d4ed8"
    elif "전략추천" in source:
        color = "#0d9488"
    elif "A:" in source or "테마" in source:
        color = "#dc2626"
    elif "B:" in source or "뉴스" in source:
        color = "#ea580c"
    elif "C:" in source or "돌파" in source:
        color = "#d97706"
    elif "D:" in source or "섹터" in source or "급락" in source or "회복" in source:
        color = "#7c3aed"
    elif "발굴" in source:
        color = "#e11d48"
    else:
        color = "#64748b"
    return (
        f'<span style="background:{color}; color:white; padding:1px 8px;'
        f' border-radius:9px; font-size:0.62em; font-weight:700;">'
        f'{source}</span>'
    )


def _render_watchlist_card(col, row: dict):
    ticker = row["ticker"]
    gain_pct = row["gain_pct"]
    gain = row["gain"]

    if gain_pct > 0:
        pct_color = "#dc2626"   # 상승: 빨강 (한국 주식 관례)
        arrow = "▲"
        gain_sign = "+"
    elif gain_pct < 0:
        pct_color = "#2563eb"   # 하락: 파랑
        arrow = "▼"
        gain_sign = ""
    else:
        pct_color = "#64748b"
        arrow = "−"
        gain_sign = ""

    mkt_color = "#1d4ed8" if row["market"] == "KOSPI" else "#16a34a" if row["market"] == "KOSDAQ" else "#64748b"
    mkt_badge = (
        f'<span style="background:{mkt_color}; color:white; padding:1px 5px;'
        f' border-radius:5px; font-size:0.65em; font-weight:700;">'
        f'{row["market"]}</span> '
    ) if row["market"] else ""
    src_badge = _source_badge_html(row.get("source", ""))

    card_html = (
        f'<div style="background:#ffffff; border-radius:12px; padding:14px;'
        f' margin-bottom:4px; border:1px solid #e2e8f0;'
        f' box-shadow:0 1px 4px rgba(0,0,0,0.06);">'
        f'<div style="font-size:0.68em; color:#94a3b8; margin-bottom:2px;">'
        f'{mkt_badge}'
        f'<span>{ticker}</span>'
        f'<span style="float:right;">{row["added_date"]} 추가</span>'
        f'</div>'
        f'<div style="font-size:1.0em; font-weight:700; color:#1e293b; margin:4px 0 2px;">'
        f'{row["name"]}'
        f'</div>'
        f'<div style="font-size:0.72em; color:#94a3b8; margin-bottom:4px;">{row["sector"]}</div>'
        f'<div style="margin-bottom:8px;">{src_badge}</div>'
        f'<div style="display:flex; justify-content:space-between; align-items:flex-end;">'
        f'  <div>'
        f'    <div style="font-size:0.62em; color:#94a3b8;">추가 당시 가격</div>'
        f'    <div style="font-size:0.9em; font-weight:600; color:#475569;">'
        f'      {row["added_price"]:,.0f}원'
        f'    </div>'
        f'  </div>'
        f'  <div style="text-align:right;">'
        f'    <div style="font-size:0.62em; color:#94a3b8;">현재가</div>'
        f'    <div style="font-size:0.9em; font-weight:600; color:#1e293b;">'
        f'      {row["current_price"]:,.0f}원'
        f'    </div>'
        f'  </div>'
        f'</div>'
        f'<div style="margin-top:8px; padding:8px 10px;'
        f' background:{pct_color}12; border-radius:8px; text-align:center;">'
        f'  <span style="font-size:1.35em; font-weight:800; color:{pct_color};">'
        f'    {arrow} {abs(gain_pct):.2f}%'
        f'  </span>'
        f'  <span style="font-size:0.78em; color:{pct_color}; font-weight:600; margin-left:8px;">'
        f'    ({gain_sign}{gain:,.0f}원/주)'
        f'  </span>'
        f'</div>'
        f'</div>'
    )

    with col:
        st.markdown(card_html, unsafe_allow_html=True)
        if st.button("🗑️ 제거", key=f"rm_wl_{ticker}", use_container_width=True):
            remove_from_watchlist(ticker)
            st.rerun()


# ─────────────────────────────────────────────────────────────────────
# 메인 렌더링
# ─────────────────────────────────────────────────────────────────────

def render_watchlist_section(daily_df: pd.DataFrame, standalone: bool = False):
    """관심종목 수익률 현황 섹션 렌더링.

    standalone=True 이면 전용 탭으로 사용 — expander 없이 풀 화면으로 렌더링.
    standalone=False(기본) 이면 인라인 expander로 렌더링.
    """
    st.markdown("## ⭐ 관심종목")

    # 로그인 안 된 경우
    if _get_username() == "default":
        st.warning(
            "⚠️ **로그인이 필요합니다.**  \n"
            "왼쪽 사이드바(🔐 로그인 패널)에서 로그인하면 이전에 저장한 관심종목을 불러올 수 있습니다.  \n"
            "페이지를 새로고침하거나 재배포 후에는 다시 로그인해주세요."
        )
        return

    items = get_watchlist()
    if not items:
        st.info("💡 발굴·트레이더 탭에서 ☆ 버튼으로 관심종목을 추가하면 여기서 수익률을 한눈에 확인할 수 있습니다.")
        return

    # 수익률 계산
    rows = []
    for item in items:
        ticker = item["ticker"]
        added_price = float(item.get("added_price", 0))
        if added_price <= 0:
            continue

        if not daily_df.empty and ticker in daily_df.index:
            row_data = daily_df.loc[ticker]
            current_price = float(row_data.get("종가", added_price))
        else:
            row_data = pd.Series(dtype=object)
            current_price = added_price  # 시세 정보 없을 때 원가 유지

        gain = current_price - added_price
        gain_pct = gain / added_price * 100 if added_price > 0 else 0.0

        rows.append({
            "ticker": ticker,
            "name": item.get("name", ticker),
            "market": item.get("market", ""),
            "sector": item.get("sector", ""),
            "source": item.get("source", ""),
            "added_date": item.get("added_date", ""),
            "added_price": added_price,
            "current_price": current_price,
            "gain": gain,
            "gain_pct": gain_pct,
            "_row_data": row_data,
        })

    if not rows:
        st.info("관심종목이 없습니다.")
        return

    # 수익률 요약 (상단 헤더)
    positive = sum(1 for r in rows if r["gain_pct"] > 0)
    negative = sum(1 for r in rows if r["gain_pct"] < 0)
    avg_pct = sum(r["gain_pct"] for r in rows) / len(rows)
    avg_color = "#dc2626" if avg_pct > 0 else "#2563eb" if avg_pct < 0 else "#64748b"
    avg_arrow = "▲" if avg_pct > 0 else "▼" if avg_pct < 0 else "−"

    summary_html = (
        f'<div style="display:flex; gap:12px; margin-bottom:12px; flex-wrap:wrap;">'
        f'  <div style="background:#f8fafc; border-radius:10px; padding:10px 18px; text-align:center; border:1px solid #e2e8f0;">'
        f'    <div style="font-size:0.68em; color:#64748b;">관심종목</div>'
        f'    <div style="font-size:1.4em; font-weight:800; color:#1e293b;">{len(rows)}개</div>'
        f'  </div>'
        f'  <div style="background:#fef2f2; border-radius:10px; padding:10px 18px; text-align:center; border:1px solid #fecaca;">'
        f'    <div style="font-size:0.68em; color:#64748b;">상승</div>'
        f'    <div style="font-size:1.4em; font-weight:800; color:#dc2626;">{positive}개</div>'
        f'  </div>'
        f'  <div style="background:#eff6ff; border-radius:10px; padding:10px 18px; text-align:center; border:1px solid #bfdbfe;">'
        f'    <div style="font-size:0.68em; color:#64748b;">하락</div>'
        f'    <div style="font-size:1.4em; font-weight:800; color:#2563eb;">{negative}개</div>'
        f'  </div>'
        f'  <div style="background:#f8fafc; border-radius:10px; padding:10px 18px; text-align:center; border:1px solid #e2e8f0;">'
        f'    <div style="font-size:0.68em; color:#64748b;">평균 수익률</div>'
        f'    <div style="font-size:1.4em; font-weight:800; color:{avg_color};">'
        f'      {avg_arrow} {abs(avg_pct):.2f}%'
        f'    </div>'
        f'  </div>'
        f'</div>'
    )
    st.markdown(summary_html, unsafe_allow_html=True)

    # 탭: 수익률 현황 / 수급 브리핑
    tab_profit, tab_supply = st.tabs(["💰 수익률 현황", "🏛️ 수급 브리핑"])

    with tab_profit:
        # 카드 그리드 (3열)
        cols_per_row = 3
        for i in range(0, len(rows), cols_per_row):
            cols = st.columns(cols_per_row)
            for j in range(cols_per_row):
                if i + j < len(rows):
                    _render_watchlist_card(cols[j], rows[i + j])

    with tab_supply:
        _render_supply_briefing(rows, daily_df)


# ─────────────────────────────────────────────────────────────────────
# 수급 브리핑
# ─────────────────────────────────────────────────────────────────────

def _render_supply_briefing(rows: list, daily_df: pd.DataFrame):
    """관심종목 수급 브리핑 — 기관·외국인·개인 순매수 현황."""
    st.caption("5일 누적 기관·외국인·개인 순매수 기반 수급 동향")

    has_supply = (
        not daily_df.empty
        and "기관합계_5일" in daily_df.columns
        and "외국인합계_5일" in daily_df.columns
    )

    if not has_supply:
        st.warning("수급 데이터가 없습니다. 데이터를 다시 로드해 주세요.")
        return

    supply_rows = []
    for r in rows:
        ticker = r["ticker"]
        if ticker not in daily_df.index:
            continue
        d = daily_df.loc[ticker]
        inst = float(d.get("기관합계_5일", 0) or 0)
        frgn = float(d.get("외국인합계_5일", 0) or 0)
        indv = float(d.get("개인_5일", 0) or 0) if "개인_5일" in daily_df.columns else 0.0
        supply_rows.append({
            "ticker": ticker,
            "name": r["name"],
            "sector": r["sector"],
            "gain_pct": r["gain_pct"],
            "current_price": r["current_price"],
            "inst": inst,
            "frgn": frgn,
            "indv": indv,
            "both": inst > 0 and frgn > 0,
        })

    if not supply_rows:
        st.info("관심종목 중 수급 데이터가 있는 종목이 없습니다.")
        return

    # ── 요약 메트릭 ──────────────────────────────────────────────
    inst_buy_n = sum(1 for s in supply_rows if s["inst"] > 0)
    frgn_buy_n = sum(1 for s in supply_rows if s["frgn"] > 0)
    both_n = sum(1 for s in supply_rows if s["both"])
    total_inst = sum(s["inst"] for s in supply_rows) / 1e8
    total_frgn = sum(s["frgn"] for s in supply_rows) / 1e8

    mc1, mc2, mc3, mc4, mc5 = st.columns(5)
    mc1.metric("🏛️ 기관 순매수", f"{inst_buy_n}개")
    mc2.metric("기관 합계", f"{total_inst:+,.0f}억")
    mc3.metric("🌍 외국인 순매수", f"{frgn_buy_n}개")
    mc4.metric("외국인 합계", f"{total_frgn:+,.0f}억")
    mc5.metric("🔥 쌍끌이", f"{both_n}개")

    st.markdown("---")

    # ── 수급 카드 ────────────────────────────────────────────────
    # 쌍끌이 → 기관 강도 → 외국인 강도 순 정렬
    supply_rows.sort(key=lambda x: (-(x["inst"] + x["frgn"])))

    for s in supply_rows:
        inst_억 = s["inst"] / 1e8
        frgn_억 = s["frgn"] / 1e8
        indv_억 = s["indv"] / 1e8

        inst_color = "#2563eb" if s["inst"] >= 0 else "#94a3b8"
        frgn_color = "#ea580c" if s["frgn"] >= 0 else "#94a3b8"
        indv_color = "#dc2626" if s["indv"] > 0 else "#2563eb" if s["indv"] < 0 else "#94a3b8"

        inst_sign = "+" if inst_억 > 0 else ""
        frgn_sign = "+" if frgn_억 > 0 else ""
        indv_sign = "+" if indv_억 > 0 else ""

        gain = s["gain_pct"]
        gain_color = "#dc2626" if gain > 0 else "#2563eb" if gain < 0 else "#64748b"
        gain_arrow = "▲" if gain > 0 else "▼" if gain < 0 else "−"

        # 쌍끌이 뱃지
        both_badge = (
            '<span style="background:#7c3aed; color:#fff; padding:1px 7px; '
            'border-radius:6px; font-size:0.65em; font-weight:700; margin-left:6px;">'
            '🔥 쌍끌이</span>'
        ) if s["both"] else ""

        # 수급 바 (기관 / 외국인 합산 강도 시각화)
        max_abs = max(abs(s["inst"]), abs(s["frgn"]), 1)
        inst_bar_w = min(100, abs(s["inst"]) / max_abs * 100)
        frgn_bar_w = min(100, abs(s["frgn"]) / max_abs * 100)

        st.markdown(
            f'<div style="background:#fff; border-radius:14px; padding:16px; '
            f'margin-bottom:10px; border:1px solid #e2e8f0; '
            f'box-shadow:0 2px 6px rgba(0,0,0,0.04);">'
            # 헤더
            f'<div style="display:flex; justify-content:space-between; align-items:center; '
            f'flex-wrap:wrap; gap:8px; margin-bottom:10px;">'
            f'  <div>'
            f'    <span style="font-weight:700; font-size:1.05em; color:#1e293b;">{s["name"]}</span>'
            f'    <span style="font-size:0.72em; color:#94a3b8; margin-left:6px;">{s["ticker"]}</span>'
            f'    {both_badge}'
            f'    <div style="font-size:0.75em; color:#64748b; margin-top:2px;">{s["sector"]}</div>'
            f'  </div>'
            f'  <div style="text-align:right;">'
            f'    <div style="font-size:0.72em; color:#94a3b8;">현재가</div>'
            f'    <div style="font-size:0.95em; font-weight:700; color:#1e293b;">'
            f'      {s["current_price"]:,.0f}원'
            f'    </div>'
            f'    <div style="font-size:0.82em; font-weight:700; color:{gain_color};">'
            f'      {gain_arrow} {abs(gain):.2f}%'
            f'    </div>'
            f'  </div>'
            f'</div>'
            # 수급 수치
            f'<div style="display:flex; gap:10px; flex-wrap:wrap; margin-bottom:10px;">'
            f'  <div style="background:#eff6ff; border-radius:8px; padding:8px 14px; flex:1; min-width:90px; text-align:center;">'
            f'    <div style="font-size:0.65em; color:#64748b;">🏛️ 기관 5일</div>'
            f'    <div style="font-size:1.05em; font-weight:800; color:{inst_color};">'
            f'      {inst_sign}{inst_억:,.1f}억</div>'
            f'  </div>'
            f'  <div style="background:#fff7ed; border-radius:8px; padding:8px 14px; flex:1; min-width:90px; text-align:center;">'
            f'    <div style="font-size:0.65em; color:#64748b;">🌍 외국인 5일</div>'
            f'    <div style="font-size:1.05em; font-weight:800; color:{frgn_color};">'
            f'      {frgn_sign}{frgn_억:,.1f}억</div>'
            f'  </div>'
            f'  <div style="background:#f8fafc; border-radius:8px; padding:8px 14px; flex:1; min-width:90px; text-align:center;">'
            f'    <div style="font-size:0.65em; color:#64748b;">👤 개인 5일</div>'
            f'    <div style="font-size:1.05em; font-weight:800; color:{indv_color};">'
            f'      {indv_sign}{indv_억:,.1f}억</div>'
            f'  </div>'
            f'</div>'
            # 강도 바
            f'<div style="font-size:0.72em; color:#64748b; margin-bottom:3px;">기관 수급 강도</div>'
            f'<div style="background:#e2e8f0; border-radius:6px; height:7px; margin-bottom:6px; overflow:hidden;">'
            f'  <div style="width:{inst_bar_w:.0f}%; height:100%; background:#2563eb; border-radius:6px;"></div>'
            f'</div>'
            f'<div style="font-size:0.72em; color:#64748b; margin-bottom:3px;">외국인 수급 강도</div>'
            f'<div style="background:#e2e8f0; border-radius:6px; height:7px; overflow:hidden;">'
            f'  <div style="width:{frgn_bar_w:.0f}%; height:100%; background:#ea580c; border-radius:6px;"></div>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
