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

def render_watchlist_section(daily_df: pd.DataFrame):
    """관심종목 수익률 현황 섹션 렌더링 (관심종목 없으면 숨김)."""
    # 로그인 안 된 경우: 항상 expander를 보여주고 로그인 안내
    if _get_username() == "default":
        with st.expander("⭐ 관심종목 수익률 현황", expanded=False):
            st.warning(
                "⚠️ **로그인이 필요합니다.**  \n"
                "왼쪽 사이드바(🔐 로그인 패널)에서 로그인하면 이전에 저장한 관심종목을 불러올 수 있습니다.  \n"
                "페이지를 새로고침하거나 재배포 후에는 다시 로그인해주세요."
            )
        return

    items = get_watchlist()
    if not items:
        return

    with st.expander(f"⭐ 관심종목 수익률 현황  ({len(items)}개)", expanded=True):
        # 수익률 계산
        rows = []
        for item in items:
            ticker = item["ticker"]
            added_price = float(item.get("added_price", 0))
            if added_price <= 0:
                continue

            if not daily_df.empty and ticker in daily_df.index:
                current_price = float(daily_df.loc[ticker].get("종가", added_price))
            else:
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

        # 카드 그리드 (3열)
        cols_per_row = 3
        for i in range(0, len(rows), cols_per_row):
            cols = st.columns(cols_per_row)
            for j in range(cols_per_row):
                if i + j < len(rows):
                    _render_watchlist_card(cols[j], rows[i + j])
