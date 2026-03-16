"""
💼 내 보유종목 포트폴리오
- 보유종목 추가/삭제 (종목코드, 매수가, 수량, 매수일)
- 현재가 기준 수익률 & 평가금액
- 종목별 기관/외국인/개인 수급 흐름
- 차트: 매수가 기준선 + 현재가 추이
- JSON 파일 영속 저장
"""

import json
import os
import datetime
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from data.fetcher import (
    get_stock_name,
    get_stock_ohlcv_history,
    get_investor_trend_individual,
    get_realtime_price,
    get_stock_news_list,
)
from analysis.indicators import calc_moving_averages

# ─────────────────────────────────────────────────────────────────────
# 파일 저장/로드
# ─────────────────────────────────────────────────────────────────────

_PORTFOLIO_BASE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "portfolio_data",
)


# ─────────────────────────────────────────────────────────────────────
# 사용자별 파일 관리
# ─────────────────────────────────────────────────────────────────────

def _get_username() -> str:
    return st.session_state.get("username", "default")


def _session_key() -> str:
    return f"my_portfolio_{_get_username()}"


def _portfolio_file() -> str:
    return os.path.join(_PORTFOLIO_BASE_DIR, f"portfolio_{_get_username()}.json")


def _ensure_dir():
    os.makedirs(_PORTFOLIO_BASE_DIR, exist_ok=True)


def _load_portfolio() -> list:
    filepath = _portfolio_file()
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
        except (json.JSONDecodeError, IOError):
            pass
    return []


def _save_portfolio(entries: list):
    _ensure_dir()
    with open(_portfolio_file(), "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)


def _get_portfolio() -> list:
    key = _session_key()
    if key not in st.session_state:
        st.session_state[key] = _load_portfolio()
    return st.session_state[key]


def _add_holding(entry: dict):
    key = _session_key()
    entries = _get_portfolio()
    entries.append(entry)
    st.session_state[key] = entries
    _save_portfolio(entries)


def _remove_holding(idx: int):
    key = _session_key()
    entries = _get_portfolio()
    if 0 <= idx < len(entries):
        entries.pop(idx)
        st.session_state[key] = entries
        _save_portfolio(entries)


# ─────────────────────────────────────────────────────────────────────
# 메인 렌더
# ─────────────────────────────────────────────────────────────────────

def _fetch_realtime_prices(holdings: list) -> dict:
    """보유종목들의 실시간 현재가를 일괄 조회해 session_state에 캐시."""
    cache_key = f"pf_realtime_{_get_username()}"
    time_key = f"pf_realtime_ts_{_get_username()}"

    # 30초 이내 재조회 방지
    now = datetime.datetime.now()
    last_ts = st.session_state.get(time_key)
    if last_ts and (now - last_ts).total_seconds() < 30:
        cached = st.session_state.get(cache_key)
        if cached:
            return cached

    result = {}
    for h in holdings:
        ticker = h["ticker"]
        info = get_realtime_price(ticker)
        result[ticker] = info

    st.session_state[cache_key] = result
    st.session_state[time_key] = now
    return result


def render_my_portfolio(daily_df: pd.DataFrame, date_str: str):
    """보유종목 포트폴리오 탭 렌더링."""
    st.markdown("## 💼 내 보유종목")

    username = _get_username()
    if username == "default":
        st.warning("⚠️ 사이드바에서 **닉네임**을 입력하면 개인별로 보유종목이 저장됩니다.")
    else:
        st.caption(f"👤 **{username}** 님의 포트폴리오")

    holdings = _get_portfolio()

    # ── 보유종목 추가 폼 ──
    _render_add_form(daily_df)

    if not holdings:
        st.info("💡 보유종목을 추가하면 수익률, 수급 흐름을 한눈에 볼 수 있습니다.")
        return

    # ── 실시간 현재가 조회 ──
    realtime = _fetch_realtime_prices(holdings)
    ts_key = f"pf_realtime_ts_{_get_username()}"
    last_ts = st.session_state.get(ts_key)
    if last_ts:
        st.caption(f"⏱️ 현재가 기준: {last_ts.strftime('%H:%M:%S')} 갱신")

    # 새로고침 버튼
    if st.button("🔄 현재가 갱신", key="pf_refresh"):
        cache_key = f"pf_realtime_{_get_username()}"
        st.session_state.pop(cache_key, None)
        st.session_state.pop(ts_key, None)
        st.rerun()

    # ── 포트폴리오 요약 ──
    _render_summary(holdings, daily_df, realtime)

    st.markdown("---")

    # ── 보유종목 브리핑 ──
    _render_portfolio_briefing(holdings, realtime)

    st.markdown("---")

    # ── 종목별 상세 분석 ──
    for i, h in enumerate(holdings):
        _render_holding_detail(i, h, daily_df, date_str)


# ─────────────────────────────────────────────────────────────────────
# 보유종목 추가 폼
# ─────────────────────────────────────────────────────────────────────

def _render_add_form(daily_df: pd.DataFrame):
    with st.expander("➕ 보유종목 추가", expanded=False):
        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
        with col1:
            # 종목 검색 (selectbox)
            stock_options = []
            if daily_df is not None and not daily_df.empty and "종목명" in daily_df.columns:
                for ticker, row in daily_df.iterrows():
                    name = row.get("종목명", ticker)
                    stock_options.append(f"{name}({ticker})")
            add_ticker_input = st.selectbox(
                "종목 선택",
                options=[""] + stock_options,
                index=0,
                key="pf_add_ticker",
            )
        with col2:
            add_price = st.number_input(
                "매수 단가(원)", min_value=0, value=0, step=100, key="pf_add_price"
            )
        with col3:
            add_qty = st.number_input(
                "수량(주)", min_value=0, value=0, step=1, key="pf_add_qty"
            )
        with col4:
            add_date = st.date_input(
                "매수일",
                value=datetime.date.today(),
                key="pf_add_date",
            )

        if st.button("✅ 추가", key="pf_add_btn", use_container_width=True, type="primary"):
            if not add_ticker_input:
                st.warning("종목을 선택해주세요.")
            elif add_price <= 0:
                st.warning("매수 단가를 입력해주세요.")
            elif add_qty <= 0:
                st.warning("수량을 입력해주세요.")
            else:
                ticker = add_ticker_input.split("(")[-1].rstrip(")")
                name = add_ticker_input.split("(")[0]
                _add_holding({
                    "ticker": ticker,
                    "name": name,
                    "buy_price": add_price,
                    "qty": add_qty,
                    "buy_date": add_date.strftime("%Y-%m-%d"),
                })
                st.success(f"✅ {name}({ticker}) 추가 완료!")
                st.rerun()


# ─────────────────────────────────────────────────────────────────────
# 포트폴리오 요약
# ─────────────────────────────────────────────────────────────────────

def _render_summary(holdings: list, daily_df: pd.DataFrame, realtime: dict = None):
    total_buy = 0
    total_eval = 0
    rows = []

    for i, h in enumerate(holdings):
        ticker = h["ticker"]
        buy_price = h["buy_price"]
        qty = h["qty"]
        buy_amount = buy_price * qty

        # 현재가 조회: 실시간 우선, 없으면 daily_df
        cur_price = 0
        change_rate = 0.0
        if realtime and ticker in realtime:
            rt = realtime[ticker]
            cur_price = rt["price"]
            change_rate = rt["change_rate"]
        if cur_price == 0 and daily_df is not None and ticker in daily_df.index:
            cur_price = int(daily_df.at[ticker, "종가"])
            change_rate = float(daily_df.at[ticker, "등락률"])

        eval_amount = cur_price * qty
        pnl = eval_amount - buy_amount
        pnl_rate = ((cur_price / buy_price) - 1) * 100 if buy_price > 0 else 0

        total_buy += buy_amount
        total_eval += eval_amount

        rows.append({
            "idx": i,
            "종목명": h.get("name", ticker),
            "티커": ticker,
            "매수가": f"{buy_price:,}",
            "현재가": f"{cur_price:,}",
            "수량": f"{qty:,}",
            "매수금액": f"{buy_amount:,.0f}",
            "평가금액": f"{eval_amount:,.0f}",
            "손익": pnl,
            "수익률": pnl_rate,
            "당일등락": change_rate,
            "매수일": h.get("buy_date", "-"),
        })

    # 전체 요약 메트릭
    total_pnl = total_eval - total_buy
    total_pnl_rate = ((total_eval / total_buy) - 1) * 100 if total_buy > 0 else 0

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("💰 총 매수금액", f"{total_buy:,.0f}원")
    with m2:
        st.metric("📊 총 평가금액", f"{total_eval:,.0f}원")
    with m3:
        pnl_color = "🔴" if total_pnl >= 0 else "🔵"
        st.metric(f"{pnl_color} 총 손익", f"{total_pnl:+,.0f}원")
    with m4:
        st.metric("📈 총 수익률", f"{total_pnl_rate:+.2f}%")

    # 종목별 테이블
    if rows:
        st.markdown("### 📋 보유종목 현황")
        for r in rows:
            pnl_val = r["손익"]
            pnl_rate_val = r["수익률"]
            day_change = r["당일등락"]

            pnl_color = "#ef4444" if pnl_val >= 0 else "#3b82f6"
            day_color = "#ef4444" if day_change >= 0 else "#3b82f6"

            st.markdown(
                f'<div style="background:#fff; border-radius:12px; padding:14px 18px; '
                f'margin-bottom:8px; border:1px solid #e2e8f0; '
                f'display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:8px;">'
                f'<div style="min-width:120px;">'
                f'<div style="font-weight:700; font-size:0.95em; color:#1e293b;">{r["종목명"]}</div>'
                f'<div style="font-size:0.75em; color:#94a3b8;">{r["티커"]} · 매수일 {r["매수일"]}</div>'
                f'</div>'
                f'<div style="text-align:center; min-width:80px;">'
                f'<div style="font-size:0.72em; color:#94a3b8;">매수가</div>'
                f'<div style="font-weight:600; font-size:0.88em;">{r["매수가"]}원</div>'
                f'</div>'
                f'<div style="text-align:center; min-width:80px;">'
                f'<div style="font-size:0.72em; color:#94a3b8;">현재가</div>'
                f'<div style="font-weight:600; font-size:0.88em;">{r["현재가"]}원</div>'
                f'</div>'
                f'<div style="text-align:center; min-width:60px;">'
                f'<div style="font-size:0.72em; color:#94a3b8;">수량</div>'
                f'<div style="font-weight:600; font-size:0.88em;">{r["수량"]}주</div>'
                f'</div>'
                f'<div style="text-align:center; min-width:100px;">'
                f'<div style="font-size:0.72em; color:#94a3b8;">평가손익</div>'
                f'<div style="font-weight:700; font-size:0.95em; color:{pnl_color};">'
                f'{pnl_val:+,.0f}원 ({pnl_rate_val:+.1f}%)</div>'
                f'</div>'
                f'<div style="text-align:center; min-width:60px;">'
                f'<div style="font-size:0.72em; color:#94a3b8;">당일</div>'
                f'<div style="font-weight:600; font-size:0.88em; color:{day_color};">'
                f'{day_change:+.2f}%</div>'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True,
            )


# ─────────────────────────────────────────────────────────────────────
# 보유종목 브리핑
# ─────────────────────────────────────────────────────────────────────

_SUPPLY_KEYWORDS = {
    "기관 매수": ["기관", "연기금", "투신", "보험", "은행", "기금"],
    "외국인 매수": ["외국인", "외인", "모건스탠리", "골드만", "JP모간", "외자"],
    "실적": ["실적", "영업이익", "매출", "순이익", "흑자", "적자", "어닝"],
    "수주/계약": ["수주", "계약", "납품", "공급", "MOU", "협약"],
    "신사업/신약": ["신약", "FDA", "임상", "승인", "허가", "신사업", "진출"],
    "배당/자사주": ["배당", "자사주", "소각", "주주환원"],
    "구조조정": ["구조조정", "인수", "합병", "M&A", "분할", "매각"],
    "정책/테마": ["정책", "규제", "테마", "대장주", "관련주", "수혜"],
}


def _analyze_supply_reason(ticker: str, name: str, realtime_info: dict) -> dict:
    """수급 데이터 + 뉴스를 분석해 브리핑 생성."""
    result = {
        "name": name,
        "ticker": ticker,
        "supply_summary": "",
        "signal": "중립",
        "signal_icon": "⚪",
        "reasons": [],
        "news": [],
    }

    # 수급 데이터
    try:
        supply = get_investor_trend_individual(ticker)
    except Exception:
        supply = pd.DataFrame()

    if not supply.empty:
        supply_sorted = supply.sort_index()
        total = supply_sorted.sum()
        inst_total = total.get("기관합계", 0) / 1e8
        frgn_total = total.get("외국인합계", 0) / 1e8
        indv_total = total.get("개인", 0) / 1e8

        # 최근일 수급
        latest = supply_sorted.iloc[-1] if len(supply_sorted) > 0 else pd.Series()
        inst_today = latest.get("기관합계", 0) / 1e8
        frgn_today = latest.get("외국인합계", 0) / 1e8

        # 수급 트렌드 판단
        parts = []
        if inst_total > 0:
            parts.append(f"기관 5일 순매수 {inst_total:+.1f}억")
        elif inst_total < 0:
            parts.append(f"기관 5일 순매도 {inst_total:+.1f}억")
        if frgn_total > 0:
            parts.append(f"외국인 5일 순매수 {frgn_total:+.1f}억")
        elif frgn_total < 0:
            parts.append(f"외국인 5일 순매도 {frgn_total:+.1f}억")

        result["supply_summary"] = " / ".join(parts) if parts else "수급 변동 미미"

        # 시그널 판단 — 기관+외국인 합산 순매수 기준 5단계
        smart_money = inst_total + frgn_total  # 기관+외국인 합산 (억)
        both_buying = inst_total > 0 and frgn_total > 0
        both_selling = inst_total < 0 and frgn_total < 0

        if smart_money > 0 and (both_buying or max(inst_total, frgn_total) > 5):
            # ── 매집 단계 (5단계) ──
            if smart_money >= 200:
                result["signal"] = "Lv5 초강력 매집"
                result["signal_icon"] = "🔴🔴🔴🔴🔴"
                result["reasons"].append(
                    f"기관+외국인 합산 순매수 {smart_money:,.0f}억 → 대형 스마트머니 집중 유입")
            elif smart_money >= 100:
                result["signal"] = "Lv4 강력 매집"
                result["signal_icon"] = "🔴🔴🔴🔴"
                result["reasons"].append(
                    f"기관+외국인 합산 순매수 {smart_money:,.0f}억 → 강한 매집 진행")
            elif smart_money >= 50:
                result["signal"] = "Lv3 적극 매집"
                result["signal_icon"] = "🔴🔴🔴"
                result["reasons"].append(
                    f"기관+외국인 합산 순매수 {smart_money:,.0f}억 → 적극적 매수세")
            elif smart_money >= 20:
                result["signal"] = "Lv2 매집 진행"
                result["signal_icon"] = "🔴🔴"
                result["reasons"].append(
                    f"기관+외국인 합산 순매수 {smart_money:,.0f}억 → 매집 흐름 감지")
            else:
                result["signal"] = "Lv1 매집 초기"
                result["signal_icon"] = "🔴"
                result["reasons"].append(
                    f"기관+외국인 합산 순매수 {smart_money:,.0f}억 → 초기 유입 단계")

            if both_buying:
                result["reasons"].append("기관+외국인 동시 순매수 (높은 신뢰도)")

        elif smart_money < 0 and (both_selling or min(inst_total, frgn_total) < -5):
            # ── 이탈 단계 (5단계) ──
            sell_amt = abs(smart_money)
            if sell_amt >= 200:
                result["signal"] = "Lv5 대량 이탈"
                result["signal_icon"] = "🔵🔵🔵🔵🔵"
                result["reasons"].append(
                    f"기관+외국인 합산 순매도 {smart_money:,.0f}억 → 대규모 이탈 경고")
            elif sell_amt >= 100:
                result["signal"] = "Lv4 강한 이탈"
                result["signal_icon"] = "🔵🔵🔵🔵"
                result["reasons"].append(
                    f"기관+외국인 합산 순매도 {smart_money:,.0f}억 → 강한 매도세")
            elif sell_amt >= 50:
                result["signal"] = "Lv3 이탈 주의"
                result["signal_icon"] = "🔵🔵🔵"
                result["reasons"].append(
                    f"기관+외국인 합산 순매도 {smart_money:,.0f}억 → 리스크 관리 필요")
            elif sell_amt >= 20:
                result["signal"] = "Lv2 이탈 진행"
                result["signal_icon"] = "🔵🔵"
                result["reasons"].append(
                    f"기관+외국인 합산 순매도 {smart_money:,.0f}억 → 매도 흐름 감지")
            else:
                result["signal"] = "Lv1 이탈 초기"
                result["signal_icon"] = "🔵"
                result["reasons"].append(
                    f"기관+외국인 합산 순매도 {smart_money:,.0f}억 → 초기 유출 단계")

            if both_selling:
                result["reasons"].append("기관+외국인 동시 순매도 (높은 경계)")

        elif inst_total > 5 or frgn_total > 5:
            who = "기관" if inst_total > frgn_total else "외국인"
            result["signal"] = "편중 매수"
            result["signal_icon"] = "🟠"
            result["reasons"].append(f"{who} 단독 순매수 {max(inst_total,frgn_total):,.0f}억 (상대측은 매도)")
        elif inst_total < -5 or frgn_total < -5:
            who = "기관" if inst_total < frgn_total else "외국인"
            result["signal"] = "편중 매도"
            result["signal_icon"] = "🟡"
            result["reasons"].append(f"{who} 단독 순매도 {min(inst_total,frgn_total):,.0f}억 → 추이 관찰")
        else:
            result["signal"] = "중립"
            result["signal_icon"] = "⚪"

        # 개인 대입 패턴 점검
        if indv_total > 10 and (inst_total < -3 or frgn_total < -3):
            result["reasons"].append("⚠️ 개인 매수 vs 기관/외국인 매도 → 물량 떠넘기기 주의")

    # 뉴스에서 이유 추출
    try:
        news_list = get_stock_news_list(ticker, count=8)
    except Exception:
        news_list = []

    if news_list:
        result["news"] = news_list[:3]  # 상위 3건
        # 뉴스 제목에서 키워드 매칭
        all_titles = " ".join(n.get("title", "") for n in news_list)
        for category, keywords in _SUPPLY_KEYWORDS.items():
            for kw in keywords:
                if kw in all_titles:
                    result["reasons"].append(f"📰 {category} 관련 뉴스 감지: '{kw}'")
                    break  # 카테고리당 1개만

    return result


def _render_portfolio_briefing(holdings: list, realtime: dict):
    """보유종목 전체 수급 브리핑."""
    st.markdown("### 📋 보유종목 수급 브리핑")

    for h in holdings:
        ticker = h["ticker"]
        name = h.get("name", ticker)
        rt = realtime.get(ticker, {})
        briefing = _analyze_supply_reason(ticker, name, rt)

        signal_icon = briefing["signal_icon"]
        signal_text = briefing["signal"]
        supply_text = briefing["supply_summary"]
        reasons = briefing["reasons"]
        news = briefing["news"]

        # 시그널별 배경색 — 매집/이탈 레벨 기반
        signal_lower = signal_text
        if "매집" in signal_lower or "편중 매수" in signal_lower:
            bg = "#fef2f2"
            border = "#fca5a5"
        elif "이탈" in signal_lower or "편중 매도" in signal_lower:
            bg = "#eff6ff"
            border = "#93c5fd"
        else:
            bg = "#f8fafc"
            border = "#e2e8f0"

        # 이유 HTML
        reasons_html = ""
        if reasons:
            reasons_li = "".join(f"<li>{r}</li>" for r in reasons)
            reasons_html = (
                f'<ul style="margin:6px 0 0 0; padding-left:18px; '
                f'font-size:0.78em; color:#475569;">{reasons_li}</ul>'
            )

        # 뉴스 HTML
        news_html = ""
        if news:
            news_items = "".join(
                f'<li><a href="{n.get("url", "#")}" target="_blank" '
                f'style="color:#4f46e5; text-decoration:none;">{n.get("title", "")}</a> '
                f'<span style="color:#94a3b8; font-size:0.85em;">({n.get("date", "")})</span></li>'
                for n in news
            )
            news_html = (
                f'<div style="margin-top:8px; padding-top:6px; border-top:1px dashed #e2e8f0;">'
                f'<div style="font-size:0.75em; color:#94a3b8; margin-bottom:3px;">📰 관련 뉴스</div>'
                f'<ul style="margin:0; padding-left:18px; font-size:0.78em;">{news_items}</ul>'
                f'</div>'
            )

        st.markdown(
            f'<div style="background:{bg}; border:1px solid {border}; '
            f'border-radius:12px; padding:14px 18px; margin-bottom:10px;">'
            f'<div style="display:flex; align-items:center; gap:8px; margin-bottom:4px;">'
            f'<span style="font-size:1.1em;">{signal_icon}</span>'
            f'<span style="font-weight:700; font-size:0.95em; color:#1e293b;">{name}</span>'
            f'<span style="background:#e2e8f0; border-radius:6px; padding:2px 8px; '
            f'font-size:0.72em; font-weight:600; color:#475569;">{signal_text}</span>'
            f'</div>'
            f'<div style="font-size:0.82em; color:#334155;">{supply_text}</div>'
            f'{reasons_html}'
            f'{news_html}'
            f'</div>',
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────────────────────────────────
# 종목별 상세 (차트 + 수급)
# ─────────────────────────────────────────────────────────────────────

def _render_holding_detail(idx: int, holding: dict, daily_df: pd.DataFrame, date_str: str):
    ticker = holding["ticker"]
    name = holding.get("name", ticker)
    buy_price = holding["buy_price"]
    buy_date = holding.get("buy_date", "")

    with st.expander(f"📊 {name} ({ticker}) 상세 분석", expanded=False):
        # 삭제 버튼
        col_del, _ = st.columns([1, 5])
        with col_del:
            if st.button("🗑️ 종목 삭제", key=f"pf_del_{idx}", type="secondary"):
                _remove_holding(idx)
                st.rerun()

        # 차트 + 수급
        _render_holding_chart(ticker, date_str, buy_price, buy_date, idx)

        # 수급 상세
        _render_supply_detail(ticker, idx)


def _render_holding_chart(ticker: str, date_str: str, buy_price: int, buy_date: str, idx: int):
    """매수가 기준선이 포함된 캔들차트 + 거래량."""
    end_dt = datetime.datetime.strptime(date_str, "%Y%m%d")

    # 매수일 기준으로 차트 시작일 결정 (매수일 -30일 또는 최소 120일)
    if buy_date:
        try:
            buy_dt = datetime.datetime.strptime(buy_date, "%Y-%m-%d")
            chart_start = buy_dt - datetime.timedelta(days=30)
        except ValueError:
            chart_start = end_dt - datetime.timedelta(days=200)
    else:
        chart_start = end_dt - datetime.timedelta(days=200)

    # 최소 120일은 보장
    min_start = end_dt - datetime.timedelta(days=200)
    if chart_start > min_start:
        chart_start = min_start

    ohlcv = get_stock_ohlcv_history(ticker, chart_start.strftime("%Y%m%d"), date_str)
    if ohlcv.empty:
        st.warning("시세 데이터를 가져올 수 없습니다.")
        return

    ohlcv = calc_moving_averages(ohlcv)
    ohlcv.index = pd.to_datetime(ohlcv.index)

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.75, 0.25],
        vertical_spacing=0.03,
    )

    # 캔들차트
    fig.add_trace(
        go.Candlestick(
            x=ohlcv.index, open=ohlcv["시가"], high=ohlcv["고가"],
            low=ohlcv["저가"], close=ohlcv["종가"],
            increasing_line_color="#ef4444", increasing_fillcolor="#ef4444",
            decreasing_line_color="#3b82f6", decreasing_fillcolor="#3b82f6",
            name="캔들",
        ),
        row=1, col=1,
    )

    # 이동평균선
    ma_colors = {"MA5": "#f59e0b", "MA20": "#10b981", "MA60": "#6366f1", "MA120": "#ec4899"}
    for ma, color in ma_colors.items():
        if ma in ohlcv.columns:
            fig.add_trace(
                go.Scatter(
                    x=ohlcv.index, y=ohlcv[ma], mode="lines",
                    line=dict(width=1, color=color), name=ma,
                ),
                row=1, col=1,
            )

    # ── 매수가 기준선 ──
    fig.add_hline(
        y=buy_price, line_dash="dash", line_color="#f97316", line_width=2,
        annotation_text=f"매수가 {buy_price:,}원",
        annotation_position="top left",
        annotation_font_color="#f97316",
        annotation_font_size=11,
        row=1, col=1,
    )

    # 매수일 세로선
    if buy_date:
        try:
            buy_dt = pd.Timestamp(buy_date)
            fig.add_vline(
                x=buy_dt.timestamp() * 1000,
                line_dash="dot", line_color="#f97316", line_width=1,
                row=1, col=1,
            )
        except Exception:
            pass

    # 거래량
    colors = ["#ef4444" if c >= o else "#3b82f6" for c, o in zip(ohlcv["종가"], ohlcv["시가"])]
    fig.add_trace(
        go.Bar(x=ohlcv.index, y=ohlcv["거래량"], marker_color=colors, name="거래량", opacity=0.6),
        row=2, col=1,
    )

    fig.update_layout(
        height=450,
        margin=dict(l=0, r=0, t=30, b=0),
        xaxis_rangeslider_visible=False,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font_size=10),
        template="plotly_white",
    )
    fig.update_xaxes(type="date")

    st.plotly_chart(fig, use_container_width=True, key=f"pf_chart_{idx}")


def _render_supply_detail(ticker: str, idx: int):
    """기관/외국인/개인 수급 흐름 차트."""
    try:
        supply = get_investor_trend_individual(ticker)
    except Exception:
        supply = pd.DataFrame()

    if supply.empty:
        st.caption("수급 데이터를 가져올 수 없습니다.")
        return

    st.markdown("#### 📊 투자자별 수급 흐름 (최근 5거래일)")

    supply.index = pd.to_datetime(supply.index)
    supply_sorted = supply.sort_index()

    # 수급 누적
    supply_cum = supply_sorted.cumsum()

    # 일별 수급 바 차트
    fig_bar = go.Figure()

    investor_colors = {
        "기관합계": "#ef4444",
        "외국인합계": "#3b82f6",
        "개인": "#94a3b8",
    }

    for col in ["기관합계", "외국인합계", "개인"]:
        if col in supply_sorted.columns:
            # 억 단위로 표시
            values = supply_sorted[col] / 1e8
            fig_bar.add_trace(
                go.Bar(
                    x=supply_sorted.index.strftime("%m/%d"),
                    y=values,
                    name=col,
                    marker_color=investor_colors.get(col, "#64748b"),
                )
            )

    fig_bar.update_layout(
        barmode="group",
        height=280,
        margin=dict(l=0, r=0, t=30, b=0),
        yaxis_title="순매수(억원)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font_size=10),
        template="plotly_white",
    )
    st.plotly_chart(fig_bar, use_container_width=True, key=f"pf_supply_bar_{idx}")

    # 누적 수급 라인
    fig_cum = go.Figure()
    for col in ["기관합계", "외국인합계", "개인"]:
        if col in supply_cum.columns:
            values = supply_cum[col] / 1e8
            fig_cum.add_trace(
                go.Scatter(
                    x=supply_cum.index.strftime("%m/%d"),
                    y=values,
                    mode="lines+markers",
                    name=f"{col} 누적",
                    line=dict(color=investor_colors.get(col, "#64748b"), width=2),
                )
            )

    fig_cum.update_layout(
        height=250,
        margin=dict(l=0, r=0, t=30, b=0),
        yaxis_title="누적 순매수(억원)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font_size=10),
        template="plotly_white",
    )
    st.plotly_chart(fig_cum, use_container_width=True, key=f"pf_supply_cum_{idx}")

    # 수급 요약 텍스트
    if not supply_sorted.empty:
        latest = supply_sorted.iloc[-1] if len(supply_sorted) > 0 else pd.Series()
        total = supply_sorted.sum()

        inst_5d = total.get("기관합계", 0) / 1e8
        frgn_5d = total.get("외국인합계", 0) / 1e8
        indv_5d = total.get("개인", 0) / 1e8

        inst_icon = "🔴" if inst_5d > 0 else "🔵"
        frgn_icon = "🔴" if frgn_5d > 0 else "🔵"
        indv_icon = "🔴" if indv_5d > 0 else "🔵"

        st.markdown(
            f'<div style="background:#f8fafc; border-radius:10px; padding:12px 16px; '
            f'border:1px solid #e2e8f0; font-size:0.85em;">'
            f'<b>5일 누적 수급:</b> '
            f'{inst_icon} 기관 {inst_5d:+,.1f}억 · '
            f'{frgn_icon} 외국인 {frgn_5d:+,.1f}억 · '
            f'{indv_icon} 개인 {indv_5d:+,.1f}억'
            f'</div>',
            unsafe_allow_html=True,
        )
