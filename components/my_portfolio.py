"""
💼 내 보유종목 포트폴리오
- 보유종목 추가/삭제 (종목코드, 매수가, 수량, 매수일)
- 현재가 기준 수익률 & 평가금액
- 종목별 기관/외국인/개인 수급 흐름
- 차트: 매수가 기준선 + 현재가 추이
- JSON 파일 영속 저장
"""

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
    clear_integration_cache,
    get_market_investor_trend,
    clear_market_investor_cache,
    is_kis_configured,
    get_kis_stock_investor,
    get_kis_access_token,
    get_kis_realtime_price,
    clear_kis_investor_cache,
    get_kis_intraday_supply,
    clear_kis_intraday_cache,
    get_kis_investor_last_error,
    get_kis_investor_last_diag,
    save_kis_credentials,
    delete_kis_credentials,
    is_kiwoom_configured,
    get_kiwoom_stock_investor,
    save_kiwoom_credentials,
    delete_kiwoom_credentials,
    get_kiwoom_investor_last_error,
    clear_kiwoom_investor_cache,
)
from analysis.indicators import calc_moving_averages

# ─────────────────────────────────────────────────────────────────────
# 파일 저장/로드
# ─────────────────────────────────────────────────────────────────────

from data.supabase_db import load_portfolio as _sb_load, save_portfolio as _sb_save


# ─────────────────────────────────────────────────────────────────────
# 사용자별 데이터 관리 (Supabase)
# ─────────────────────────────────────────────────────────────────────

def _get_username() -> str:
    return st.session_state.get("username", "default")


def _session_key() -> str:
    return f"my_portfolio_{_get_username()}"


def _load_portfolio() -> list:
    return _sb_load(_get_username())


def _save_portfolio(entries: list):
    _sb_save(_get_username(), entries)


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
    kis_ok = is_kis_configured()
    for h in holdings:
        ticker = h["ticker"]
        if kis_ok:
            kis = get_kis_realtime_price(ticker)
            if kis and kis.get("price", 0) > 0:
                result[ticker] = {"price": kis["price"], "change_rate": kis["change_rate"], "name": h.get("name", ticker)}
                continue
        result[ticker] = get_realtime_price(ticker)

    st.session_state[cache_key] = result
    st.session_state[time_key] = now
    return result


def render_my_portfolio(daily_df: pd.DataFrame, date_str: str):
    """보유종목 포트폴리오 탭 렌더링."""
    st.markdown("## 💼 내 보유종목")

    username = _get_username()
    if username == "default":
        st.warning(
            "⚠️ **로그인이 필요합니다.**  \n"
            "왼쪽 사이드바(🔐 로그인 패널)에서 로그인하면 이전에 저장한 보유종목을 불러올 수 있습니다.  \n"
            "페이지를 새로고침하거나 재배포 후에는 다시 로그인해주세요."
        )
        return
    else:
        st.caption(f"👤 **{username}** 님의 포트폴리오")

    holdings = _get_portfolio()

    # ── 보유종목 추가 폼 ──
    _render_add_form(daily_df)

    # ── KIS API 설정 ──
    _render_kis_settings()

    if not holdings:
        st.info("💡 보유종목을 추가하면 수익률, 수급 흐름을 한눈에 볼 수 있습니다.")
        return

    # ── 실시간 현재가 조회 ──
    realtime = _fetch_realtime_prices(holdings)
    ts_key = f"pf_realtime_ts_{_get_username()}"
    last_ts = st.session_state.get(ts_key)
    if last_ts:
        st.caption(f"⏱️ 현재가 기준: {last_ts.strftime('%H:%M:%S')} 갱신  ·  수급(기관/외국인)은 장 마감 후 확정 반영")

    # 새로고침 버튼 — 현재가 + 수급 동시 갱신
    if st.button("🔄 현재가 갱신", key="pf_refresh"):
        cache_key = f"pf_realtime_{_get_username()}"
        st.session_state.pop(cache_key, None)
        st.session_state.pop(ts_key, None)
        # 수급 캐시 전체 클리어 (Naver integration + KIS 장중)
        for h in holdings:
            clear_integration_cache(h["ticker"])
            clear_kis_investor_cache(h["ticker"])
            clear_kis_intraday_cache(h["ticker"])
        clear_market_investor_cache()
        st.rerun()

    # ── 시장 전체 수급 (장중 실시간) ──
    _render_market_investor_trend()

    # ── 포트폴리오 요약 ──
    _render_summary(holdings, daily_df, realtime)


    st.markdown("---")

    # ── 수급 브리핑 새로고침 버튼 ──
    if st.button("🧹 수급 브리핑 새로고침", key="pf_supply_refresh"):
        for h in holdings:
            clear_integration_cache(h["ticker"])
            clear_kis_investor_cache(h["ticker"])
            clear_kis_intraday_cache(h["ticker"])
        clear_market_investor_cache()
        st.success("수급 데이터가 새로고침되었습니다. (최신 데이터로 재분석)")
        st.rerun()

    # ── 보유종목 브리핑 ──
    _render_portfolio_briefing(holdings, realtime)

    st.markdown("---")

    # ── 종목별 상세 분석 ──
    for i, h in enumerate(holdings):
        _render_holding_detail(i, h, daily_df, date_str)


# ─────────────────────────────────────────────────────────────────────
# KIS API 설정 UI
# ─────────────────────────────────────────────────────────────────────

def _render_kis_settings():
    """KIS API 자격증명을 앱 UI에서 직접 입력/저장/삭제."""
    configured = is_kis_configured()
    label = "🔴 KIS API 연결됨 — 장중 실시간 수급 활성화" if configured else "⚙️ KIS API 설정 (장중 실시간 수급)"

    with st.expander(label, expanded=not configured):
        if configured:
            st.success("✅ KIS API 키가 설정되어 있습니다. 보유종목 기관/외국인 수급이 장중 실시간으로 표시됩니다.")
            col_t, col_d = st.columns([1, 1])
            with col_t:
                if st.button("🔧 연결 테스트", key="kis_test", type="secondary"):
                    with st.spinner("KIS API 테스트 중..."):
                        tok = get_kis_access_token()
                        if not tok:
                            st.error("❌ 토큰 발급 실패 — App Key / App Secret을 확인해주세요.")
                        else:
                            st.success(f"✅ 토큰 발급 성공: {tok[:20]}...")
                            import requests as _req
                            from data.fetcher import _get_kis_credentials, KIS_API_BASE
                            app_key, app_secret = _get_kis_credentials()
                            hdrs = {
                                "Authorization": f"Bearer {tok}",
                                "appkey": app_key,
                                "appsecret": app_secret,
                                "custtype": "P",
                                "content-type": "application/json; charset=utf-8",
                            }
                            # Test: 주식현재가 투자자 (FHKST01010900) — LS머트리얼즈 전체 값 덤프
                            try:
                                import datetime as _dt
                                today_str = _dt.date.today().strftime("%Y%m%d")
                                r = _req.get(
                                    f"{KIS_API_BASE}/uapi/domestic-stock/v1/quotations/inquire-investor",
                                    headers={**hdrs, "content-type": "application/json; charset=utf-8",
                                             "tr_id": "FHKST01010900"},
                                    params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": "417200"},
                                    timeout=10,
                                )
                                data = r.json()
                                rt_cd = data.get("rt_cd", "?")
                                msg = data.get("msg1", "")
                                if rt_cd == "0":
                                    raw = data.get("output") or data.get("output1")
                                    st.success(f"✅ FHKST01010900 성공 (LS머트리얼즈) | 최상위 키: {list(data.keys())}")
                                    if isinstance(raw, list):
                                        # 날짜 내림차순 정렬 후 최근 3행 표시
                                        rows = sorted(
                                            [r2 for r2 in raw if isinstance(r2, dict)],
                                            key=lambda x: x.get("stck_bsop_date", ""), reverse=True
                                        )
                                        st.caption(f"총 {len(rows)}행 | 최근 3행 전체 값:")
                                        for row in rows[:3]:
                                            st.code(
                                                f"날짜: {row.get('stck_bsop_date')}\n"
                                                f"  종가:          {row.get('stck_clpr')}\n"
                                                f"  외국인 순매수수량: {row.get('frgn_ntby_qty')}\n"
                                                f"  기관계 순매수수량: {row.get('orgn_ntby_qty')}\n"
                                                f"  개인  순매수수량: {row.get('prsn_ntby_qty')}\n"
                                                f"  외국인 순매수대금: {row.get('frgn_ntby_tr_pbmn')}\n"
                                                f"  기관계 순매수대금: {row.get('orgn_ntby_tr_pbmn')}\n"
                                                f"  개인  순매수대금: {row.get('prsn_ntby_tr_pbmn')}\n"
                                                f"  외국인 매수대금:  {row.get('frgn_shnu_tr_pbmn')}\n"
                                                f"  외국인 매도대금:  {row.get('frgn_seln_tr_pbmn')}\n"
                                                f"  기관계 매수대금:  {row.get('orgn_shnu_tr_pbmn')}\n"
                                                f"  기관계 매도대금:  {row.get('orgn_seln_tr_pbmn')}\n"
                                                f"  개인  매수대금:  {row.get('prsn_shnu_tr_pbmn')}\n"
                                                f"  개인  매도대금:  {row.get('prsn_seln_tr_pbmn')}"
                                            )
                                    elif isinstance(raw, dict):
                                        st.code(str(raw))
                                else:
                                    st.error(f"❌ [FHKST01010900] rt_cd={rt_cd}: {msg}")
                                    st.code(str(data)[:400])
                            except Exception as e:
                                st.error(f"❌ [FHKST01010900] 요청 실패: {e}")
                            # Test 2: 기본시세 확인
                            try:
                                r2 = _req.get(
                                    f"{KIS_API_BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
                                    headers={**hdrs, "tr_id": "FHKST01010100"},
                                    params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": "417200"},
                                    timeout=10,
                                )
                                data2 = r2.json()
                                rt2 = data2.get("rt_cd", "?")
                                if rt2 == "0":
                                    price = data2.get("output", {}).get("stck_prpr", "N/A")
                                    st.success(f"✅ [기본시세] LS머트리얼즈 현재가: {price}원")
                                else:
                                    st.error(f"❌ [FHKST01010100] rt_cd={rt2}: {data2.get('msg1','')}")
                            except Exception as e:
                                st.error(f"❌ [FHKST01010100] 요청 실패: {e}")
                if st.button("🗑️ KIS 키 삭제", key="kis_delete", type="secondary"):
                    delete_kis_credentials()
                    st.success("삭제됐습니다.")
                    st.rerun()
        else:
            st.markdown(
                "**한국투자증권 Open API** 앱 키를 입력하면 보유종목의 기관/외국인 수급을 **장중 실시간**으로 표시합니다.\n\n"
                "📌 발급: [apiportal.koreainvestment.com](https://apiportal.koreainvestment.com) → 로그인 → 앱 등록 → 앱 키 발급"
            )
            app_key = st.text_input(
                "App Key",
                placeholder="PSxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                key="kis_app_key_input",
                type="password",
            )
            app_secret = st.text_input(
                "App Secret",
                placeholder="xxxxxxxxxxxxxxxx...",
                key="kis_app_secret_input",
                type="password",
            )
            if st.button("💾 저장", key="kis_save", type="primary"):
                if not app_key or not app_secret:
                    st.warning("App Key와 App Secret을 모두 입력해주세요.")
                elif len(app_key) < 20 or len(app_secret) < 20:
                    st.warning("키 형식이 올바르지 않습니다. 다시 확인해주세요.")
                else:
                    save_kis_credentials(app_key.strip(), app_secret.strip())
                    st.success("✅ KIS API 키가 저장됐습니다! 이제 장중 실시간 수급이 표시됩니다.")
                    st.rerun()

    # ── 키움 REST API 설정 ──
    with st.expander("🟠 키움 REST API 설정 (KIS 미사용 시 대체 수급 소스)", expanded=False):
        if is_kiwoom_configured():
            st.success("✅ 키움 REST API 연결됨")
            col_a, col_b = st.columns(2)
            with col_a:
                st.caption("기관/외국인 일별 순매매 데이터 (ka10009 TR)")
            with col_b:
                if st.button("🗑️ 키움 키 삭제", key="kiwoom_delete", type="secondary"):
                    delete_kiwoom_credentials()
                    st.success("삭제됐습니다.")
                    st.rerun()
        else:
            st.markdown(
                "**키움 REST API** 앱 키를 입력하면 KIS 미연결 환경에서도 기관/외국인 수급을 조회합니다.\n\n"
                "📌 발급: [openapi.kiwoom.com](https://openapi.kiwoom.com) → 로그인 → API 사용신청 → 앱 키 발급"
            )
            kw_app_key = st.text_input(
                "App Key",
                placeholder="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                key="kiwoom_app_key_input",
                type="password",
            )
            kw_app_secret = st.text_input(
                "App Secret",
                placeholder="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                key="kiwoom_app_secret_input",
                type="password",
            )
            if st.button("💾 저장", key="kiwoom_save", type="primary"):
                if not kw_app_key or not kw_app_secret:
                    st.warning("App Key와 App Secret을 모두 입력해주세요.")
                else:
                    save_kiwoom_credentials(kw_app_key.strip(), kw_app_secret.strip())
                    st.success("✅ 키움 API 키가 저장됐습니다!")
                    st.rerun()


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
                    mkt = row.get("시장", "") if "시장" in row.index else ""
                    mkt_prefix = f"[{mkt}] " if mkt else ""
                    stock_options.append(f"{mkt_prefix}{name}({ticker})")
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
                # "[KOSPI] 삼성전자" or "삼성전자" 형태 모두 처리
                raw_name = add_ticker_input.split("(")[0]
                name = raw_name.split("] ")[-1] if "] " in raw_name else raw_name
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
# 시장 전체 투자자 수급 (장중 실시간)
# ─────────────────────────────────────────────────────────────────────

def _render_market_investor_trend():
    """KOSPI/KOSDAQ 시장 전체 기관/외국인/개인 순매매 — 장중 실시간 표시."""
    trend = get_market_investor_trend()
    if not trend or not trend.get("bizdate"):
        return

    bizdate = trend["bizdate"]
    is_today = trend.get("is_today", False)
    date_label = f"{bizdate[4:6]}/{bizdate[6:8]}"
    status_label = "🟢 장중 실시간" if is_today else f"📅 {date_label} 확정"

    def _fmt(v: float) -> str:
        return f"{v:+,.0f}억"

    def _color(v: float) -> str:
        return "#ef4444" if v >= 0 else "#3b82f6"

    def _bar(v: float, max_abs: float = 10000) -> str:
        pct = min(abs(v) / max_abs * 100, 100)
        color = "#ef4444" if v >= 0 else "#3b82f6"
        return (
            f'<div style="background:#f1f5f9; border-radius:4px; height:6px; margin-top:3px;">'
            f'<div style="width:{pct:.1f}%; background:{color}; height:6px; border-radius:4px;"></div>'
            f'</div>'
        )

    kospi = trend.get("kospi", {})
    kosdaq = trend.get("kosdaq", {})
    total = trend.get("total", {})

    max_abs = max(
        abs(total.get("personal", 0)),
        abs(total.get("foreign", 0)),
        abs(total.get("institution", 0)),
        1,
    )

    def _cell(label: str, v: float) -> str:
        return (
            f'<div style="text-align:center; min-width:90px;">'
            f'<div style="font-size:0.7em; color:#94a3b8; margin-bottom:2px;">{label}</div>'
            f'<div style="font-weight:700; font-size:0.92em; color:{_color(v)};">{_fmt(v)}</div>'
            f'{_bar(v, max_abs)}'
            f'</div>'
        )

    rows_html = ""
    for mkt_label, mkt in [("KOSPI", kospi), ("KOSDAQ", kosdaq), ("합산", total)]:
        bold = "font-weight:700;" if mkt_label == "합산" else ""
        border_top = "border-top:1px solid #e2e8f0; padding-top:10px; margin-top:6px;" if mkt_label == "합산" else ""
        rows_html += (
            f'<div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap; {border_top}">'
            f'<div style="min-width:52px; font-size:0.8em; {bold} color:#475569;">{mkt_label}</div>'
            f'{_cell("개인", mkt.get("personal", 0))}'
            f'{_cell("외국인", mkt.get("foreign", 0))}'
            f'{_cell("기관", mkt.get("institution", 0))}'
            f'</div>'
        )

    st.markdown(
        f'<div style="background:#f8fafc; border:1px solid #e2e8f0; border-radius:12px; '
        f'padding:14px 18px; margin:10px 0 16px 0;">'
        f'<div style="display:flex; align-items:center; gap:8px; margin-bottom:10px;">'
        f'<span style="font-weight:700; font-size:0.9em; color:#1e293b;">📊 시장 수급</span>'
        f'<span style="font-size:0.72em; color:#64748b; background:#e2e8f0; border-radius:6px; '
        f'padding:2px 8px;">{status_label}</span>'
        f'</div>'
        f'{rows_html}'
        f'<div style="font-size:0.68em; color:#94a3b8; margin-top:8px;">'
        f'단위: 억원 · 순매수(빨강) / 순매도(파랑) · {"장중 1분 갱신 기준" if is_today else "전일 확정 기준"}'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


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

        # 최근 확정일 기관/외국인 수급 조회 (Naver) — 5일 누적 계산
        inst_5d = 0.0
        frgn_5d = 0.0
        inst_buy_5d = 0.0   # 양수일 합계
        inst_sell_5d = 0.0  # 음수일 합계(절댓값)
        frgn_buy_5d = 0.0
        frgn_sell_5d = 0.0
        supply_date_str = ""
        try:
            supply = get_investor_trend_individual(ticker)
            if not supply.empty:
                supply_sorted = supply.sort_index()
                supply_date_str = supply_sorted.index[-1].strftime("%m/%d")
                for val in supply_sorted["기관합계"] / 1e8:
                    if val >= 0:
                        inst_buy_5d += val
                    else:
                        inst_sell_5d += abs(val)
                for val in supply_sorted["외국인합계"] / 1e8:
                    if val >= 0:
                        frgn_buy_5d += val
                    else:
                        frgn_sell_5d += abs(val)
                inst_5d = inst_buy_5d - inst_sell_5d
                frgn_5d = frgn_buy_5d - frgn_sell_5d
        except Exception:
            pass

        # KIS 수급: 장중 가집계(FHPTJ04400000) 우선 → 없으면 FHKST01010900(전일 확정)
        kis_live = False
        if is_kis_configured():
            # 1순위: 장중 가집계 (09:30/10:00/11:20/13:20/14:30 업데이트)
            try:
                intraday = get_kis_intraday_supply(ticker)
                if intraday:
                    fq = intraday.get("외국인_qty", 0)
                    oq = intraday.get("기관_qty", 0)
                    bucket = intraday.get("bucket", "")
                    # qty × 현재가 → 억원
                    if cur_price > 0:
                        frgn_5d = fq * cur_price / 1e8
                        inst_5d = oq * cur_price / 1e8
                    else:
                        frgn_5d = 0.0
                        inst_5d = 0.0
                    inst_buy_5d = inst_5d if inst_5d >= 0 else 0.0
                    inst_sell_5d = abs(inst_5d) if inst_5d < 0 else 0.0
                    frgn_buy_5d = frgn_5d if frgn_5d >= 0 else 0.0
                    frgn_sell_5d = abs(frgn_5d) if frgn_5d < 0 else 0.0
                    supply_date_str = f"가집계 {bucket}"
                    kis_live = True
            except Exception:
                pass

            # 2순위: FHKST01010900 (전일 확정 데이터)
            if not kis_live:
                try:
                    kis = get_kis_stock_investor(ticker)
                    if kis:
                        inst_5d = kis.get("기관", 0.0)
                        frgn_5d = kis.get("외국인", 0.0)
                        inst_buy_5d = inst_5d if inst_5d >= 0 else 0.0
                        inst_sell_5d = abs(inst_5d) if inst_5d < 0 else 0.0
                        frgn_buy_5d = frgn_5d if frgn_5d >= 0 else 0.0
                        frgn_sell_5d = abs(frgn_5d) if frgn_5d < 0 else 0.0
                        import datetime as _dt
                        _today = _dt.date.today().strftime("%Y%m%d")
                        _kis_date = kis.get("date", "")
                        if _kis_date == _today:
                            supply_date_str = "오늘"
                        elif _kis_date and len(_kis_date) == 8:
                            supply_date_str = f"{_kis_date[4:6]}/{_kis_date[6:8]}"
                        else:
                            supply_date_str = "KIS"
                        kis_live = True
                except Exception:
                    pass

        # 3순위: 키움 REST API (KIS 미연결 시 대체 소스)
        kiwoom_live = False
        if not kis_live and is_kiwoom_configured():
            try:
                kw = get_kiwoom_stock_investor(ticker)
                if kw:
                    fq = kw.get("외국인_qty", 0)
                    oq = kw.get("기관_qty", 0)
                    # qty × 현재가 → 억원 (수량 기반)
                    if cur_price > 0:
                        frgn_5d = fq * cur_price / 1e8
                        inst_5d = oq * cur_price / 1e8
                    else:
                        frgn_5d = float(fq) / 1e4  # 단위 미상시 만주 단위로 근사
                        inst_5d = float(oq) / 1e4
                    inst_buy_5d = inst_5d if inst_5d >= 0 else 0.0
                    inst_sell_5d = abs(inst_5d) if inst_5d < 0 else 0.0
                    frgn_buy_5d = frgn_5d if frgn_5d >= 0 else 0.0
                    frgn_sell_5d = abs(frgn_5d) if frgn_5d < 0 else 0.0
                    _kw_date = kw.get("date", "")
                    import datetime as _dt
                    _today = _dt.date.today().strftime("%Y%m%d")
                    if _kw_date == _today:
                        supply_date_str = "오늘(키움)"
                    elif _kw_date and len(_kw_date) == 8:
                        supply_date_str = f"{_kw_date[4:6]}/{_kw_date[6:8]}(키움)"
                    else:
                        supply_date_str = "키움"
                    kiwoom_live = True
            except Exception:
                pass

        # daily_df에서 시장 조회
        mkt = ""
        if daily_df is not None and ticker in daily_df.index and "시장" in daily_df.columns:
            mkt = str(daily_df.at[ticker, "시장"])

        rows.append({
            "idx": i,
            "종목명": h.get("name", ticker),
            "티커": ticker,
            "시장": mkt,
            "매수가": f"{buy_price:,}",
            "현재가": f"{cur_price:,}",
            "수량": f"{qty:,}",
            "매수금액": f"{buy_amount:,.0f}",
            "평가금액": f"{eval_amount:,.0f}",
            "손익": pnl,
            "수익률": pnl_rate,
            "당일등락": change_rate,
            "매수일": h.get("buy_date", "-"),
            "기관최근": inst_5d,
            "외국인최근": frgn_5d,
            "기관매수": inst_buy_5d,
            "기관매도": inst_sell_5d,
            "외국인매수": frgn_buy_5d,
            "외국인매도": frgn_sell_5d,
            "수급기준일": supply_date_str,
            "수급실시간": kis_live or kiwoom_live,
            "수급소스": "KIS" if kis_live else ("키움" if kiwoom_live else "Naver"),
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
        # 연결 상태 배지
        if is_kis_configured():
            any_kis_live = any(r.get("수급실시간") for r in rows)
            if any_kis_live:
                st.caption("🔴 KIS API 연결됨 · 기관/외국인 수급 당일 실시간 (1분 주기)")
            else:
                err = get_kis_investor_last_error()
                if err:
                    st.caption(f"🔴 KIS API 연결됨 · ⚠️ 수급 TR 오류: {err}")
                else:
                    st.caption("🔴 KIS API 연결됨 · 수급 조회 중 (장 시작 전이거나 조회 대기)")
        elif is_kiwoom_configured():
            any_kw_live = any(r.get("수급소스") == "키움" for r in rows)
            if any_kw_live:
                st.caption("🟠 키움 REST API 연결됨 · 기관/외국인 수급 일별 순매매 (5분 주기)")
            else:
                err = get_kiwoom_investor_last_error()
                st.caption(f"🟠 키움 REST API 연결됨 · {'⚠️ ' + err if err else '수급 조회 중'}")
        else:
            st.caption(
                "💡 KIS/키움 API 미연결 · 수급은 네이버 기준 (KRX 배치 집계, 당일 실시간 미지원) | "
                "연결: Secrets에 [kis] 또는 [kiwoom] app_key / app_secret 추가"
            )

        for r in rows:
            pnl_val = r["손익"]
            pnl_rate_val = r["수익률"]
            day_change = r["당일등락"]
            inst_d = r["기관최근"]
            frgn_d = r["외국인최근"]
            inst_buy = r.get("기관매수", 0.0)
            inst_sell = r.get("기관매도", 0.0)
            frgn_buy = r.get("외국인매수", 0.0)
            frgn_sell = r.get("외국인매도", 0.0)
            sup_date = r["수급기준일"]
            sup_live = r.get("수급실시간", False)

            pnl_color = "#ef4444" if pnl_val >= 0 else "#3b82f6"
            day_color = "#ef4444" if day_change >= 0 else "#3b82f6"

            # 0.05억(500만원) 미만은 중립으로 처리 — -0.0 표시 방지
            _THRESHOLD = 0.05
            inst_neutral = abs(inst_d) < _THRESHOLD
            frgn_neutral = abs(frgn_d) < _THRESHOLD
            inst_d_disp = 0.0 if inst_neutral else inst_d
            frgn_d_disp = 0.0 if frgn_neutral else frgn_d
            inst_color = "#94a3b8" if inst_neutral else ("#ef4444" if inst_d > 0 else "#3b82f6")
            frgn_color = "#94a3b8" if frgn_neutral else ("#ef4444" if frgn_d > 0 else "#3b82f6")

            # 수급 라벨: 실시간이면 당일, 아니면 5일 누적
            live_dot = '<span style="color:#ef4444;">●</span> ' if sup_live else ""
            inst_period = "당일" if sup_live else f"5일({sup_date}까지)"
            frgn_period = "당일" if sup_live else f"5일({sup_date}까지)"
            inst_trend = "중립" if inst_neutral else ("진입중" if inst_d > 0 else "이탈중")
            frgn_trend = "중립" if frgn_neutral else ("진입중" if frgn_d > 0 else "이탈중")
            inst_trend_color = "#94a3b8" if inst_neutral else ("#ef4444" if inst_d > 0 else "#3b82f6")
            frgn_trend_color = "#94a3b8" if frgn_neutral else ("#ef4444" if frgn_d > 0 else "#3b82f6")

            # 매수/매도 breakdown HTML (실시간이 아닐 때만 표시)
            if not sup_live and (inst_buy > 0 or inst_sell > 0):
                inst_breakdown = (
                    f'<div style="font-size:0.68em; color:#94a3b8; margin-top:2px;">'
                    f'매수 <span style="color:#ef4444;">+{inst_buy:.1f}</span> '
                    f'매도 <span style="color:#3b82f6;">-{inst_sell:.1f}</span>'
                    f'</div>'
                )
            else:
                inst_breakdown = ""
            if not sup_live and (frgn_buy > 0 or frgn_sell > 0):
                frgn_breakdown = (
                    f'<div style="font-size:0.68em; color:#94a3b8; margin-top:2px;">'
                    f'매수 <span style="color:#ef4444;">+{frgn_buy:.1f}</span> '
                    f'매도 <span style="color:#3b82f6;">-{frgn_sell:.1f}</span>'
                    f'</div>'
                )
            else:
                frgn_breakdown = ""

            _mkt = r.get("시장", "")
            _mkt_color = "#1d4ed8" if _mkt == "KOSPI" else "#16a34a" if _mkt == "KOSDAQ" else "#94a3b8"
            _mkt_badge = (
                f'<span style="background:{_mkt_color}; color:white; padding:1px 5px;'
                f' border-radius:5px; font-size:0.72em; font-weight:700; margin-left:4px;">'
                f'{_mkt}</span>'
            ) if _mkt else ""

            st.markdown(
                f'<div style="background:#fff; border-radius:12px; padding:14px 18px; '
                f'margin-bottom:8px; border:1px solid #e2e8f0; '
                f'display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:8px;">'
                f'<div style="min-width:120px;">'
                f'<div style="font-weight:700; font-size:0.95em; color:#1e293b;">{r["종목명"]}{_mkt_badge}</div>'
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
                f'<div style="text-align:center; min-width:100px;">'
                f'<div style="font-size:0.72em; color:#94a3b8;">{live_dot}기관 {inst_period}</div>'
                f'<div style="font-weight:700; font-size:0.88em; color:{inst_color};">'
                f'{inst_d_disp:+.1f}억</div>'
                f'<div style="font-size:0.7em; font-weight:600; color:{inst_trend_color};">{inst_trend}</div>'
                f'{inst_breakdown}'
                f'</div>'
                f'<div style="text-align:center; min-width:100px;">'
                f'<div style="font-size:0.72em; color:#94a3b8;">{live_dot}외국인 {frgn_period}</div>'
                f'<div style="font-weight:700; font-size:0.88em; color:{frgn_color};">'
                f'{frgn_d_disp:+.1f}억</div>'
                f'<div style="font-size:0.7em; font-weight:600; color:{frgn_trend_color};">{frgn_trend}</div>'
                f'{frgn_breakdown}'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
            # 삭제 확인 상태
            confirm_key = f"pf_del_confirm_{r['idx']}"
            if st.session_state.get(confirm_key):
                col_yes, col_no, _ = st.columns([1, 1, 6])
                with col_yes:
                    if st.button("✅ 삭제 확인", key=f"pf_del_yes_{r['idx']}", type="primary"):
                        _remove_holding(r['idx'])
                        st.session_state.pop(confirm_key, None)
                        st.rerun()
                with col_no:
                    if st.button("취소", key=f"pf_del_no_{r['idx']}", type="secondary"):
                        st.session_state.pop(confirm_key, None)
                        st.rerun()
            else:
                col_btn, _ = st.columns([1, 9])
                with col_btn:
                    if st.button("🗑️ 삭제", key=f"pf_del_{r['idx']}", type="secondary"):
                        st.session_state[confirm_key] = True
                        st.rerun()


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
        "opinion": "",
        "opinion_icon": "",
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

        # 최근 확정일 수급
        latest = supply_sorted.iloc[-1] if len(supply_sorted) > 0 else pd.Series()
        inst_latest = latest.get("기관합계", 0) / 1e8
        frgn_latest = latest.get("외국인합계", 0) / 1e8
        latest_date = supply_sorted.index[-1].strftime("%m/%d") if len(supply_sorted) > 0 else ""

        # 수급 트렌드 판단
        # 최근 확정일 수급
        latest_parts = []
        latest_parts.append(f"기관 {inst_latest:+.1f}억")
        latest_parts.append(f"외국인 {frgn_latest:+.1f}억")
        today_line = f"📌 최근({latest_date}): " + " / ".join(latest_parts)

        # 5일 누적
        parts = []
        if inst_total > 0:
            parts.append(f"기관 5일 순매수 {inst_total:+.1f}억")
        elif inst_total < 0:
            parts.append(f"기관 5일 순매도 {inst_total:+.1f}억")
        if frgn_total > 0:
            parts.append(f"외국인 5일 순매수 {frgn_total:+.1f}억")
        elif frgn_total < 0:
            parts.append(f"외국인 5일 순매도 {frgn_total:+.1f}억")
        five_day_line = "📊 5일 누적: " + (" / ".join(parts) if parts else "변동 미미")

        result["supply_summary"] = today_line + "<br>" + five_day_line

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

        # ── 매매 의견 ──
        sig = result["signal"]
        if "Lv5" in sig and "매집" in sig:
            result["opinion"] = "적극 매수"
            result["opinion_icon"] = "🟢"
        elif "Lv4" in sig and "매집" in sig:
            result["opinion"] = "매수"
            result["opinion_icon"] = "🟢"
        elif "Lv3" in sig and "매집" in sig:
            result["opinion"] = "매수 관심"
            result["opinion_icon"] = "🟢"
        elif "Lv2" in sig and "매집" in sig:
            result["opinion"] = "관망(매수 대기)"
            result["opinion_icon"] = "🟡"
        elif "Lv1" in sig and "매집" in sig:
            result["opinion"] = "관망"
            result["opinion_icon"] = "🟡"
        elif sig == "편중 매수":
            result["opinion"] = "관망(편중 주의)"
            result["opinion_icon"] = "🟡"
        elif sig == "중립":
            result["opinion"] = "매도 검토"
            result["opinion_icon"] = "🟠"
            result["reasons"].append("💡 수급 변동 미미 → 모멘텀 부재, 매도 타이밍 검토")
        elif sig == "편중 매도":
            result["opinion"] = "매도 검토"
            result["opinion_icon"] = "🟠"
            result["reasons"].append("💡 한쪽 매도 흐름 → 추가 이탈 가능성 대비")
        elif "Lv1" in sig and "이탈" in sig:
            result["opinion"] = "매도 검토"
            result["opinion_icon"] = "🟠"
        elif "Lv2" in sig and "이탈" in sig:
            result["opinion"] = "매도"
            result["opinion_icon"] = "🔴"
            result["reasons"].append("💡 매도 흐름 본격화 → 손절/익절 고려")
        elif "Lv3" in sig and "이탈" in sig:
            result["opinion"] = "적극 매도"
            result["opinion_icon"] = "🔴"
            result["reasons"].append("💡 상당한 이탈 → 비중 축소 권장")
        elif "Lv4" in sig and "이탈" in sig:
            result["opinion"] = "즉시 매도"
            result["opinion_icon"] = "🔴"
            result["reasons"].append("💡 강한 매도세 → 빠른 대응 필요")
        elif "Lv5" in sig and "이탈" in sig:
            result["opinion"] = "즉시 매도"
            result["opinion_icon"] = "🔴"
            result["reasons"].append("🚨 대규모 이탈 → 즉시 전량 매도 고려")

        # 개인 물량 떠넘기기 패턴 → 의견 하향
        if indv_total > 10 and (inst_total < -3 or frgn_total < -3):
            if "매수" in result.get("opinion", ""):
                result["opinion"] = "관망(수급 괴리)"
                result["opinion_icon"] = "🟡"

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
    """보유종목 전체 수급 브리핑 — 항상 최신 수급 데이터 기반."""
    st.markdown("### 📋 보유종목 수급 브리핑")
    st.caption("ℹ️ 수급 데이터는 장 마감 후 확정됩니다. 장중에는 전 거래일 기준입니다.")

    # 캐시 클리어 → 최신 수급 데이터 강제 조회
    for h in holdings:
        clear_integration_cache(h["ticker"])

    for h in holdings:
        ticker = h["ticker"]
        name = h.get("name", ticker)
        rt = realtime.get(ticker, {})
        briefing = _analyze_supply_reason(ticker, name, rt)

        signal_icon = briefing["signal_icon"]
        signal_text = briefing["signal"]
        supply_text = briefing["supply_summary"]
        opinion = briefing.get("opinion", "")
        opinion_icon = briefing.get("opinion_icon", "")
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

        # 의견 배지 색상
        op_bg_map = {"🟢": "#dcfce7", "🟡": "#fef9c3", "🟠": "#ffedd5", "🔴": "#fee2e2"}
        op_fg_map = {"🟢": "#166534", "🟡": "#854d0e", "🟠": "#9a3412", "🔴": "#991b1b"}
        op_bg = op_bg_map.get(opinion_icon, "#f1f5f9")
        op_fg = op_fg_map.get(opinion_icon, "#475569")

        # 의견 HTML
        opinion_html = ""
        if opinion:
            opinion_html = (
                f'<span style="background:{op_bg}; color:{op_fg}; border-radius:6px; '
                f'padding:3px 10px; font-size:0.78em; font-weight:700; '
                f'margin-left:6px;">{opinion_icon} {opinion}</span>'
            )

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
            f'<div style="display:flex; align-items:center; gap:8px; margin-bottom:4px; flex-wrap:wrap;">'
            f'<span style="font-size:1.1em;">{signal_icon}</span>'
            f'<span style="font-weight:700; font-size:0.95em; color:#1e293b;">{name}</span>'
            f'<span style="background:#e2e8f0; border-radius:6px; padding:2px 8px; '
            f'font-size:0.72em; font-weight:600; color:#475569;">{signal_text}</span>'
            f'{opinion_html}'
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
            if st.button("🗑️ 종목 삭제", key=f"pf_detail_del_{idx}", type="secondary"):
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

    # 수급 요약 테이블
    if not supply_sorted.empty:
        total = supply_sorted.sum()

        inst_buy_total = supply_sorted["기관합계"].clip(lower=0).sum() / 1e8
        inst_sell_total = supply_sorted["기관합계"].clip(upper=0).abs().sum() / 1e8
        inst_net = inst_buy_total - inst_sell_total
        frgn_buy_total = supply_sorted["외국인합계"].clip(lower=0).sum() / 1e8
        frgn_sell_total = supply_sorted["외국인합계"].clip(upper=0).abs().sum() / 1e8
        frgn_net = frgn_buy_total - frgn_sell_total
        indv_buy_total = supply_sorted["개인"].clip(lower=0).sum() / 1e8
        indv_sell_total = supply_sorted["개인"].clip(upper=0).abs().sum() / 1e8
        indv_net = indv_buy_total - indv_sell_total

        def trend_badge(net):
            if net > 0:
                return '<span style="background:#fee2e2; color:#ef4444; border-radius:4px; padding:1px 6px; font-size:0.85em; font-weight:700;">▲ 진입중</span>'
            elif net < 0:
                return '<span style="background:#dbeafe; color:#3b82f6; border-radius:4px; padding:1px 6px; font-size:0.85em; font-weight:700;">▼ 이탈중</span>'
            else:
                return '<span style="background:#f1f5f9; color:#64748b; border-radius:4px; padding:1px 6px; font-size:0.85em;">― 중립</span>'

        def net_color(v):
            return "#ef4444" if v >= 0 else "#3b82f6"

        st.markdown(
            f'<div style="background:#f8fafc; border-radius:10px; padding:14px 16px; '
            f'border:1px solid #e2e8f0; font-size:0.83em; margin-top:4px;">'
            f'<div style="font-weight:700; color:#1e293b; margin-bottom:10px;">📋 5일 수급 요약</div>'
            f'<table style="width:100%; border-collapse:collapse;">'
            f'<thead><tr style="color:#64748b; font-size:0.85em;">'
            f'<th style="text-align:left; padding:4px 8px;">구분</th>'
            f'<th style="text-align:right; padding:4px 8px; color:#ef4444;">순매수(+합계)</th>'
            f'<th style="text-align:right; padding:4px 8px; color:#3b82f6;">순매도(-합계)</th>'
            f'<th style="text-align:right; padding:4px 8px;">순포지션</th>'
            f'<th style="text-align:center; padding:4px 8px;">상태</th>'
            f'</tr></thead>'
            f'<tbody>'
            f'<tr style="border-top:1px solid #e2e8f0;">'
            f'<td style="padding:6px 8px; font-weight:600;">기관</td>'
            f'<td style="text-align:right; padding:6px 8px; color:#ef4444;">+{inst_buy_total:.1f}억</td>'
            f'<td style="text-align:right; padding:6px 8px; color:#3b82f6;">-{inst_sell_total:.1f}억</td>'
            f'<td style="text-align:right; padding:6px 8px; font-weight:700; color:{net_color(inst_net)};">{inst_net:+.1f}억</td>'
            f'<td style="text-align:center; padding:6px 8px;">{trend_badge(inst_net)}</td>'
            f'</tr>'
            f'<tr style="border-top:1px solid #f1f5f9;">'
            f'<td style="padding:6px 8px; font-weight:600;">외국인</td>'
            f'<td style="text-align:right; padding:6px 8px; color:#ef4444;">+{frgn_buy_total:.1f}억</td>'
            f'<td style="text-align:right; padding:6px 8px; color:#3b82f6;">-{frgn_sell_total:.1f}억</td>'
            f'<td style="text-align:right; padding:6px 8px; font-weight:700; color:{net_color(frgn_net)};">{frgn_net:+.1f}억</td>'
            f'<td style="text-align:center; padding:6px 8px;">{trend_badge(frgn_net)}</td>'
            f'</tr>'
            f'<tr style="border-top:1px solid #f1f5f9;">'
            f'<td style="padding:6px 8px; font-weight:600;">개인</td>'
            f'<td style="text-align:right; padding:6px 8px; color:#ef4444;">+{indv_buy_total:.1f}억</td>'
            f'<td style="text-align:right; padding:6px 8px; color:#3b82f6;">-{indv_sell_total:.1f}억</td>'
            f'<td style="text-align:right; padding:6px 8px; font-weight:700; color:{net_color(indv_net)};">{indv_net:+.1f}억</td>'
            f'<td style="text-align:center; padding:6px 8px;">{trend_badge(indv_net)}</td>'
            f'</tr>'
            f'</tbody></table>'
            f'<div style="font-size:0.75em; color:#94a3b8; margin-top:8px;">'
            f'순매수(+합계): 양수인 날의 합 / 순매도(-합계): 음수인 날의 합 / 순포지션 = 순매수 - 순매도'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
