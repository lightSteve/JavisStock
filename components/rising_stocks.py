"""
상승 종목 분석 컴포넌트 — 카드 그리드 + 상세 패널 UI

흐름:
  1) 상승 종목을 카드 그리드(4열)로 한눈에 표시
  2) 카드 클릭 → 해당 종목 상세 패널이 아래에 펼쳐짐
  3) 상세 패널: 기간 선택(5/10/20/60일) + 캔들차트 + 일별 테이블 + 수급 + 실적
"""

from typing import Optional
import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.fetcher import (
    get_weekly_volume_trend,
    get_stock_fundamentals,
    get_stock_ohlcv_history,
    get_investor_trend_individual,
    get_latest_trading_date,
    _fetch_stock_integration,
)
from analysis.indicators import calc_all_indicators, get_technical_summary

# 한 행에 보여줄 카드 수 (데스크톱 3열, 모바일 CSS로 2열)
_COLS_PER_ROW = 3


# ═══════════════════════════════════════════════════════════════════════════
# 진입점
# ═══════════════════════════════════════════════════════════════════════════

def render_rising_stocks(daily_df: pd.DataFrame) -> Optional[str]:
    """상승 종목 분석 메인. 선택한 종목 티커를 반환."""
    st.markdown("## 📊 상승 종목 분석")

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return None

    sub1, sub2, sub3 = st.tabs([
        "📦 ETF/ETN TOP20",
        "🏛️ KOSPI TOP50",
        "🚀 KOSDAQ TOP50",
    ])

    selected = None
    with sub1:
        r = _render_tab(daily_df, tab_key="etf", mode="etf_etn", top_n=20)
        if r:
            selected = r
    with sub2:
        r = _render_tab(daily_df, tab_key="kospi", mode="stock", market="KOSPI", top_n=50)
        if r:
            selected = r
    with sub3:
        r = _render_tab(daily_df, tab_key="kosdaq", mode="stock", market="KOSDAQ", top_n=50)
        if r:
            selected = r

    return selected


# ═══════════════════════════════════════════════════════════════════════════
# 통합 탭 렌더러
# ═══════════════════════════════════════════════════════════════════════════

def _render_tab(
    daily_df: pd.DataFrame,
    tab_key: str,
    mode: str = "stock",          # "etf_etn" | "stock"
    market: str = "",             # "KOSPI" | "KOSDAQ" (mode=stock 일 때)
    top_n: int = 50,
) -> Optional[str]:
    """
    탭 하나를 렌더링: 필터 → 요약 메트릭 → 카드 그리드 → 상세 패널.
    """

    # ── 데이터 필터 ───────────────────────────────────────────────
    df = daily_df.copy()
    if mode == "etf_etn":
        if "종목유형" not in df.columns:
            st.warning("종목유형 데이터가 없습니다. 데이터를 다시 로드해 주세요.")
            return None
        df = df[df["종목유형"].isin(["etf", "etn"])]
        label = "ETF/ETN"
    else:
        if "종목유형" in df.columns:
            df = df[df["종목유형"] == "stock"]
        if "시장" in df.columns and market:
            df = df[df["시장"] == market]
        label = market

    if df.empty:
        st.info(f"{label} 데이터가 없습니다.")
        return None

    # ── 상승 종목 필터 + 정렬 ─────────────────────────────────────
    rising = df[df["등락률"] > 0].copy()
    if rising.empty:
        st.info(f"오늘 상승한 {label} 종목이 없습니다.")
        return None

    if mode == "stock":
        rising = _compute_momentum_score(rising)
    else:
        rising = rising.sort_values("등락률", ascending=False)

    rising = rising[~rising.index.duplicated(keep='first')]
    top = rising.head(top_n)
    total_rising = len(rising)

    # ── 요약 메트릭 ───────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(f"📊 {label} 전체", f"{len(df):,}개")
    c2.metric("📈 상승 종목", f"{total_rising:,}개")
    c3.metric("평균 상승률", f"{rising['등락률'].mean():+.2f}%")
    c4.metric("🏆 최대 상승률", f"{rising['등락률'].max():+.2f}%")

    st.markdown("---")

    # ── 보기 모드 선택 ────────────────────────────────────────────
    view_col1, view_col2 = st.columns([2, 1])
    with view_col1:
        st.markdown(
            f"<span style='font-size:1.1em; font-weight:700; color:#1e293b;'>"
            f"상승률 상위 {len(top)}개 종목</span>",
            unsafe_allow_html=True,
        )
    with view_col2:
        view_mode = st.radio(
            "보기", ["🃏 카드", "📋 테이블"],
            horizontal=True, key=f"view_mode_{tab_key}",
            label_visibility="collapsed",
        )

    # ── 카드 그리드 또는 테이블 ───────────────────────────────────
    sess_key = f"selected_{tab_key}"

    if view_mode == "🃏 카드":
        _render_card_grid(top, sess_key=sess_key, tab_key=tab_key, show_score=(mode == "stock"))
    else:
        _render_table_view(top, sess_key=sess_key, tab_key=tab_key, mode=mode)

    # ── 선택된 종목 상세 패널 ─────────────────────────────────────
    sel_ticker = st.session_state.get(sess_key)
    if sel_ticker:
        _render_detail_panel(sel_ticker, tab_key=tab_key)
        return sel_ticker

    return None


# ═══════════════════════════════════════════════════════════════════════════
# 카드 그리드
# ═══════════════════════════════════════════════════════════════════════════

def _render_card_grid(
    df: pd.DataFrame,
    sess_key: str,
    tab_key: str,
    show_score: bool = False,
):
    """상승 종목을 4열 카드 그리드로 렌더링. 클릭 시 session_state에 티커 저장."""
    tickers = df.index.tolist()

    for row_start in range(0, len(tickers), _COLS_PER_ROW):
        cols = st.columns(_COLS_PER_ROW)
        for j, col in enumerate(cols):
            idx = row_start + j
            if idx >= len(tickers):
                break
            ticker = tickers[idx]
            r = df.iloc[idx]

            name = r.get("종목명", ticker)
            price = int(r.get("종가", 0))
            change = r.get("등락률", 0)
            volume = int(r.get("거래량", 0))
            score = r.get("모멘텀점수", None)

            # 색상
            chg_color = "#dc2626" if change > 0 else "#2563eb" if change < 0 else "#64748b"
            is_selected = st.session_state.get(sess_key) == ticker
            border = f"2px solid #4f46e5" if is_selected else "1px solid #e2e8f0"
            bg = "#f0f0ff" if is_selected else "#ffffff"

            with col:
                # 카드 HTML
                score_html = ""
                if show_score and score is not None:
                    score_html = (
                        f'<div style="font-size:0.78em; color:#6366f1; margin-top:3px; font-weight:600;">'
                        f'모멘텀 {score:.0f}</div>'
                    )

                vol_str = f"{volume/10000:.0f}만" if volume >= 10000 else f"{volume:,}"
                arrow = '▲' if change > 0 else '▼' if change < 0 else '─'

                # 수급 값 안전하게 미리 계산
                _inst_v = r.get('기관합계_5일', 0)
                _frgn_v = r.get('외국인합계_5일', 0)
                if pd.isna(_inst_v):
                    _inst_v = 0
                if pd.isna(_frgn_v):
                    _frgn_v = 0
                supply_html = (
                    f'<div style="margin-top:5px; font-size:0.78em;">'
                    f'<span style="color:#2563eb; font-weight:600;">🏛️{_inst_v / 1e8:+,.0f}억</span>'
                    f'&nbsp;'
                    f'<span style="color:#ea580c; font-weight:600;">🌍{_frgn_v / 1e8:+,.0f}억</span>'
                    f'</div>'
                )

                card_html = (
                    f'<div style="background:{bg}; border-radius:12px; padding:14px;'
                    f' border:{border}; margin-bottom:4px;'
                    f' box-shadow:0 1px 4px rgba(0,0,0,0.05); min-height:120px;">'
                    f'<div style="font-size:0.78em; color:#94a3b8; margin-bottom:2px;">{ticker}</div>'
                    f'<div style="font-size:0.98em; font-weight:700; color:#1e293b;'
                    f' white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">{name}</div>'
                    f'<div style="display:flex; justify-content:space-between; align-items:baseline; margin-top:7px;">'
                    f'<span style="font-size:1.08em; font-weight:700; color:{chg_color};">{price:,}</span>'
                    f'<span style="font-size:0.92em; font-weight:600; color:{chg_color};">{arrow}{abs(change):.2f}%</span>'
                    f'</div>'
                    f'<div style="font-size:0.78em; color:#94a3b8; margin-top:3px;">거래량 {vol_str}</div>'
                    f'{score_html}'
                    f'{supply_html}'
                    f'</div>'
                )

                st.markdown(card_html, unsafe_allow_html=True)

                # 버튼: 상세 보기
                st.button(
                    "📈 상세" if not is_selected else "✅ 선택됨",
                    key=f"card_{tab_key}_{ticker}",
                    use_container_width=True,
                    type="primary" if is_selected else "secondary",
                    on_click=lambda t=ticker: st.session_state.update({sess_key: t}),
                )


# ═══════════════════════════════════════════════════════════════════════════
# 테이블 보기 (기존 방식 유지)
# ═══════════════════════════════════════════════════════════════════════════

def _render_table_view(
    df: pd.DataFrame,
    sess_key: str,
    tab_key: str,
    mode: str,
):
    """테이블 보기 + 행 선택 셀렉트박스."""
    disp = _build_display_df(df, show_type=(mode == "etf_etn"), show_score=(mode == "stock"))
    st.dataframe(
        disp,
        use_container_width=True,
        hide_index=True,
        height=min(len(disp) * 38 + 50, 750),
        column_config={
            "당일등락률(%)": st.column_config.NumberColumn(format="%+.2f%%"),
            "모멘텀점수": st.column_config.NumberColumn(format="%.1f"),
            "거래량": st.column_config.NumberColumn(format="%d"),
        },
    )

    # 종목 선택
    tickers = df.index.tolist()
    names = [df.loc[t, "종목명"] if "종목명" in df.columns else t for t in tickers]
    options = [f"{t} - {n}" for t, n in zip(tickers, names)]

    def _on_table_select():
        val = st.session_state.get(f"tbl_sel_{tab_key}", "선택하세요...")
        if val != "선택하세요...":
            st.session_state[sess_key] = val.split(" - ")[0].strip()

    st.selectbox(
        "🔍 종목 선택",
        ["선택하세요..."] + options,
        index=0,
        key=f"tbl_sel_{tab_key}",
        on_change=_on_table_select,
    )


# ═══════════════════════════════════════════════════════════════════════════
# 상세 패널 (선택된 종목)
# ═══════════════════════════════════════════════════════════════════════════

def _render_detail_panel(ticker: str, tab_key: str):
    """선택된 종목의 상세 흐름 패널."""
    st.markdown("---")

    # 기간 선택 + 닫기 버튼
    hd1, hd2, hd3 = st.columns([3, 1, 1])
    with hd1:
        st.markdown(
            f"<span style='font-size:1.15em; font-weight:700; color:#4f46e5;'>"
            f"📈 종목 상세 — {ticker}</span>",
            unsafe_allow_html=True,
        )
    with hd2:
        view_days = st.selectbox(
            "기간",
            [5, 10, 20, 60],
            index=0,
            format_func=lambda x: f"{x}일",
            key=f"days_{tab_key}",
            label_visibility="collapsed",
        )
    with hd3:
        st.button(
            "✕ 닫기", key=f"close_{tab_key}",
            on_click=lambda: st.session_state.update({f"selected_{tab_key}": None}),
        )

    _render_period_detail(ticker, view_days, tab_key)


def _render_period_detail(ticker: str, days: int, tab_key: str):
    """최근 N거래일 상세 흐름: 가격·거래량·수급·기술지표."""
    import datetime as _dt

    end = get_latest_trading_date()
    end_dt = _dt.datetime.strptime(end, "%Y%m%d")
    margin = int(days * 1.8) + 10
    start_dt = end_dt - _dt.timedelta(days=margin)
    start = start_dt.strftime("%Y%m%d")

    hist = get_stock_ohlcv_history(ticker, start, end)
    if hist.empty or len(hist) < 2:
        st.warning("최근 가격 데이터를 가져올 수 없습니다.")
        return

    recent = hist.tail(days).copy()
    inv = get_investor_trend_individual(ticker)
    fund = get_stock_fundamentals(ticker)
    hist_full = calc_all_indicators(hist)
    tech_summary = get_technical_summary(hist_full)

    name = ticker
    try:
        integ = _fetch_stock_integration(ticker)
        name = integ.get("stockName", ticker)
    except Exception:
        pass

    last = recent.iloc[-1]
    first = recent.iloc[0]
    day_change = last.get("등락률", 0)
    period_change = (last["종가"] - first["종가"]) / first["종가"] * 100 if first["종가"] > 0 else 0
    color = "#dc2626" if day_change > 0 else "#2563eb" if day_change < 0 else "#64748b"
    pc = "#dc2626" if period_change > 0 else "#2563eb" if period_change < 0 else "#64748b"

    # ─── 헤더 카드 ────
    rsi_color = '#dc2626' if tech_summary.get('RSI') in ('과매수','강세') else '#2563eb' if tech_summary.get('RSI') in ('과매도','약세') else '#475569'

    st.markdown(
        f"""<div style="
            background:#ffffff; border-radius:14px; padding:20px;
            border:1px solid #e2e8f0; box-shadow:0 2px 8px rgba(0,0,0,0.06);
        ">
            <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap;">
                <div>
                    <span style="font-size:0.8em; color:#94a3b8;">{ticker}</span>
                    <div style="font-size:1.3em; font-weight:700; color:#1e293b;">{name}</div>
                </div>
                <div style="text-align:right;">
                    <div style="font-size:1.5em; font-weight:700; color:{color};">{int(last['종가']):,}원</div>
                    <div style="font-size:0.88em; color:{color};">
                        당일 {'▲' if day_change > 0 else '▼' if day_change < 0 else '─'} {abs(day_change):.2f}%
                    </div>
                    <div style="font-size:0.88em; color:{pc};">
                        {days}일간 {'▲' if period_change > 0 else '▼' if period_change < 0 else '─'} {abs(period_change):.2f}%
                    </div>
                </div>
            </div>
            <div style="margin-top:10px; display:flex; gap:12px; flex-wrap:wrap; font-size:0.8em; color:#475569;">
                <span>PER <b>{fund.get('PER','N/A')}</b></span>
                <span>PBR <b>{fund.get('PBR','N/A')}</b></span>
                <span>EPS <b>{fund.get('EPS','N/A')}</b></span>
                <span>배당 <b>{fund.get('배당수익률','N/A')}</b></span>
                <span style="border-left:1px solid #cbd5e1; padding-left:12px;">
                    RSI <b style="color:{rsi_color};">{tech_summary.get('RSI값','N/A')} ({tech_summary.get('RSI','N/A')})</b>
                </span>
                <span>MACD <b>{tech_summary.get('MACD','N/A')}</b></span>
                <span>정배열 <b>{tech_summary.get('정배열','N/A')}</b></span>
                <span>볼린저 <b>{tech_summary.get('볼린저','N/A')}</b></span>
            </div>
        </div>""",
        unsafe_allow_html=True,
    )

    st.markdown("")

    # ─── 차트 + 수급 ────
    col_chart, col_inv = st.columns([3, 2])

    with col_chart:
        _render_period_chart(recent, name, days, tab_key)

    with col_inv:
        if not inv.empty:
            st.markdown("**🏦 투자자별 동향** (최대 5일)")
            recent_inv = inv.tail(5)
            inv_disp = recent_inv.copy()
            inv_disp.index = inv_disp.index.strftime("%m/%d")
            for col_name in inv_disp.columns:
                inv_disp[col_name] = inv_disp[col_name].apply(
                    lambda x: f"{x/1e8:+,.0f}억" if abs(x) >= 1e8 else f"{x/1e4:+,.0f}만"
                )
            st.dataframe(inv_disp, use_container_width=True)
            if days > 5:
                st.caption("⚠️ 수급은 Naver API 제한으로 최대 5일")
        else:
            st.info("투자자 동향 데이터가 없습니다.")

        # 분기 실적
        quarters = fund.get("분기실적", [])
        actual_q = [q for q in quarters if not q.get("추정", False)]
        if actual_q:
            st.markdown("**📊 분기별 실적**")
            show_q = actual_q[-4:]
            q_df = pd.DataFrame(show_q)[["분기", "매출액", "영업이익"]]
            q_df.columns = ["분기", "매출액(억)", "영업이익(억)"]
            st.dataframe(q_df, use_container_width=True, hide_index=True)

    # ─── 일별 상세 테이블 (접기 가능) ────
    with st.expander(f"📅 최근 {len(recent)}거래일 일별 상세", expanded=(days <= 10)):
        day_rows = []
        for dt_idx, row in recent.iterrows():
            dt_label = dt_idx.strftime("%m/%d (%a)")
            o, h, l, c, v = int(row["시가"]), int(row["고가"]), int(row["저가"]), int(row["종가"]), int(row["거래량"])
            chg = row.get("등락률", 0)
            candle = "양봉 🔴" if c > o else ("음봉 🔵" if c < o else "보합 ─")
            day_rows.append({
                "날짜": dt_label, "시가": f"{o:,}", "고가": f"{h:,}",
                "저가": f"{l:,}", "종가": f"{c:,}",
                "등락률": f"{chg:+.2f}%", "거래량": f"{v:,}", "캔들": candle,
            })
        st.dataframe(pd.DataFrame(day_rows), use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════
# 차트
# ═══════════════════════════════════════════════════════════════════════════

def _render_period_chart(df_period: pd.DataFrame, name: str, days: int, tab_key: str):
    """N일 캔들스틱 + 거래량 차트."""
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.7, 0.3], vertical_spacing=0.08,
    )
    colors = [
        "#dc2626" if r["종가"] >= r["시가"] else "#2563eb"
        for _, r in df_period.iterrows()
    ]
    fig.add_trace(
        go.Candlestick(
            x=df_period.index, open=df_period["시가"], high=df_period["고가"],
            low=df_period["저가"], close=df_period["종가"],
            increasing_line_color="#dc2626", decreasing_line_color="#2563eb", name="가격",
        ), row=1, col=1,
    )
    fig.add_trace(
        go.Bar(x=df_period.index, y=df_period["거래량"], marker_color=colors, name="거래량", opacity=0.7),
        row=2, col=1,
    )
    fig.update_layout(
        title=dict(text=f"{name} 최근 {days}일", font=dict(size=14)),
        height=max(300, min(480, days * 6 + 220)),
        showlegend=False, xaxis_rangeslider_visible=False,
        template="plotly_white", margin=dict(l=10, r=10, t=35, b=10),
    )
    fig.update_xaxes(type="category", tickformat="%m/%d")
    st.plotly_chart(fig, use_container_width=True, key=f"chart_{tab_key}_{days}")


# ═══════════════════════════════════════════════════════════════════════════
# 유틸리티
# ═══════════════════════════════════════════════════════════════════════════

def _compute_momentum_score(df: pd.DataFrame) -> pd.DataFrame:
    """종합 모멘텀 점수 = 등락률(40%) + 거래대금(30%) + 수급(30%)."""
    df = df.copy()
    val_range = df["등락률"].max() - df["등락률"].min()
    df["_등락률z"] = (df["등락률"] - df["등락률"].min()) / max(val_range, 0.01) * 100
    df["_거래대금r"] = df["거래대금"].rank(ascending=True, pct=True) * 100
    supply_cols = [c for c in df.columns if "기관" in c and "5일" in c]
    frgn_cols = [c for c in df.columns if "외국인" in c and "5일" in c]
    df["_수급합"] = 0
    for c in supply_cols + frgn_cols:
        df["_수급합"] = df["_수급합"] + df[c].fillna(0)
    df["_수급z"] = df["_수급합"].rank(ascending=True, pct=True) * 100 if df["_수급합"].std() > 0 else 50
    df["모멘텀점수"] = df["_등락률z"] * 0.4 + df["_거래대금r"] * 0.3 + df["_수급z"] * 0.3
    df = df.sort_values("모멘텀점수", ascending=False)
    df = df.drop(columns=[c for c in df.columns if c.startswith("_")], errors="ignore")
    return df


def _build_display_df(
    df: pd.DataFrame, show_type: bool = False, show_score: bool = False,
) -> pd.DataFrame:
    """테이블 보기용 DataFrame."""
    rows = []
    for ticker, r in df.iterrows():
        d = {
            "티커": ticker,
            "종목명": r.get("종목명", ticker),
            "현재가": f'{int(r.get("종가", 0)):,}',
            "당일등락률(%)": r.get("등락률", 0),
            "거래량": int(r.get("거래량", 0)),
            "거래대금(억)": f'{r.get("거래대금", 0) / 1e8:,.0f}',
        }
        if show_score and "모멘텀점수" in r:
            d["모멘텀점수"] = round(r["모멘텀점수"], 1)
        if show_type:
            d["유형"] = str(r.get("종목유형", "")).upper()
        # 수급 정보 추가
        if "기관합계_5일" in r.index:
            d["기관(억)"] = round(r.get("기관합계_5일", 0) / 1e8, 1)
        if "외국인합계_5일" in r.index:
            d["외국인(억)"] = round(r.get("외국인합계_5일", 0) / 1e8, 1)
        if "업종" in r and r.get("업종"):
            d["업종"] = r.get("업종", "")
        rows.append(d)
    return pd.DataFrame(rows)
