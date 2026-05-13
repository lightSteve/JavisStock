"""
📈 거래량 급증 발굴 종목 컴포넌트

전일대비 거래량 증가율 기준 Top 30 종목을 표시합니다.

알고리즘:
- 전일 스냅샷과 오늘 daily_df 비교 → 거래량 증가율 계산
- 거래량 증가율 상위 30개 종목 표시
- 4배(400%) 이상 급증 종목 강조 표시 (추세 지속 패턴)
"""

import os
import streamlit as st
import pandas as pd
from typing import Optional

from data.fetcher import load_daily_snapshot, list_available_snapshots
from components.watchlist import get_watchlist, add_to_watchlist, remove_from_watchlist

# 거래량 급증 기준: 전일 대비 몇 배 이상이면 강조
_SURGE_4X_RATIO = 4.0   # 4배 이상: 추세 지속 확률 높음
_SURGE_2X_RATIO = 2.0   # 2배 이상: 주목 구간
_TOP_N = 30              # 상위 몇 개 종목 표시
_COLS_PER_ROW = 3


# ─────────────────────────────────────────────────────────────────────
# 전일 거래량 로드
# ─────────────────────────────────────────────────────────────────────

def _load_prev_volume(today_date: str, market: str = "ALL") -> pd.Series:
    """
    전일 스냅샷에서 거래량 Series 반환.
    반환: index=티커, values=전일거래량 (int)
    """
    available = list_available_snapshots(market)
    # today_date보다 이전 날짜만 추출, 가장 최근 선택
    prev_dates = sorted([d for d in available if d < today_date], reverse=True)
    if not prev_dates:
        return pd.Series(dtype=float)

    prev_date = prev_dates[0]
    prev_df = load_daily_snapshot(prev_date, market)
    if prev_df.empty or "거래량" not in prev_df.columns:
        return pd.Series(dtype=float)

    return prev_df["거래량"].astype(float)


# ─────────────────────────────────────────────────────────────────────
# 메인 계산 함수
# ─────────────────────────────────────────────────────────────────────

def compute_volume_surge(
    daily_df: pd.DataFrame,
    date_str: str,
    top_n: int = _TOP_N,
) -> pd.DataFrame:
    """
    전일 거래량 대비 오늘 거래량 증가율을 계산해 상위 top_n 종목 반환.

    반환 컬럼:
      - 거래량 (오늘)
      - 전일거래량
      - 거래량증가율 (% 단위)
      - 거래량배수 (몇 배)
      - 기타 daily_df 컬럼 유지
    """
    if daily_df.empty or "거래량" not in daily_df.columns:
        return pd.DataFrame()

    prev_vol = _load_prev_volume(date_str)

    df = daily_df.copy()
    df["거래량"] = df["거래량"].fillna(0).astype(float)

    if prev_vol.empty:
        # 전일 데이터 없으면 20일 평균 거래량 대비로 fallback (OHLCV 없으면 스킵)
        return pd.DataFrame()

    # 전일 거래량 합류
    df["전일거래량"] = prev_vol.reindex(df.index).fillna(0).astype(float)

    # 0 거래량 제거
    df = df[df["거래량"] > 0]
    df = df[df["전일거래량"] > 0]

    # 거래량 배수 및 증가율
    df["거래량배수"] = df["거래량"] / df["전일거래량"]
    df["거래량증가율"] = (df["거래량배수"] - 1) * 100

    # 주식 종목만 필터 (ETF/ETN 제외 가능, 선택적)
    if "종목유형" in df.columns:
        df = df[df["종목유형"] == "stock"]

    # 상위 top_n 반환
    return df.nlargest(top_n, "거래량배수").copy()


# ─────────────────────────────────────────────────────────────────────
# 카드 렌더링 헬퍼
# ─────────────────────────────────────────────────────────────────────

def _surge_badge(ratio: float) -> str:
    """거래량 배수에 따른 배지 HTML."""
    if ratio >= _SURGE_4X_RATIO:
        bg, fg = "#fef3c7", "#92400e"
        label = f"🔥 {ratio:.1f}배 급증"
    elif ratio >= _SURGE_2X_RATIO:
        bg, fg = "#dcfce7", "#166534"
        label = f"📈 {ratio:.1f}배"
    else:
        bg, fg = "#f1f5f9", "#475569"
        label = f"↑ {ratio:.1f}배"
    return (
        f'<span style="background:{bg}; color:{fg}; padding:2px 8px;'
        f' border-radius:8px; font-size:0.72em; font-weight:700;">{label}</span>'
    )


def _border_color(ratio: float) -> str:
    if ratio >= _SURGE_4X_RATIO:
        return "#f59e0b"   # 황금색: 4배 이상 급증
    elif ratio >= _SURGE_2X_RATIO:
        return "#22c55e"   # 녹색: 2배 이상
    return "#e2e8f0"       # 기본


def _render_surge_card(ticker: str, row: pd.Series, rank: int, tab_key: str):
    """개별 거래량 급증 카드."""
    name = str(row.get("종목명", ticker))
    price = int(row.get("종가", 0))
    change = float(row.get("등락률", 0))
    ratio = float(row.get("거래량배수", 1.0))
    today_vol = int(row.get("거래량", 0))
    prev_vol = int(row.get("전일거래량", 0))
    market = str(row.get("시장", ""))
    inst = float(row.get("기관합계_5일", 0) or 0)
    frgn = float(row.get("외국인합계_5일", 0) or 0)

    chg_color = "#dc2626" if change > 0 else "#2563eb" if change < 0 else "#64748b"
    arrow = "▲" if change > 0 else "▼" if change < 0 else "─"
    border = _border_color(ratio)

    mkt_color = "#1d4ed8" if market == "KOSPI" else "#16a34a" if market == "KOSDAQ" else "#94a3b8"
    mkt_badge = (
        f'<span style="background:{mkt_color}; color:white; padding:1px 5px;'
        f' border-radius:5px; font-size:0.7em; font-weight:700;">{market}</span> '
    ) if market else ""

    surge_badge = _surge_badge(ratio)

    vol_today_str = f"{today_vol / 10000:.0f}만" if today_vol >= 10000 else f"{today_vol:,}"
    vol_prev_str = f"{prev_vol / 10000:.0f}만" if prev_vol >= 10000 else f"{prev_vol:,}"

    inst_str = f"{inst / 1e8:+,.0f}억"
    frgn_str = f"{frgn / 1e8:+,.0f}억"
    inst_color = "#2563eb" if inst >= 0 else "#6b7280"
    frgn_color = "#ea580c" if frgn >= 0 else "#6b7280"

    rank_icons = {1: "🥇", 2: "🥈", 3: "🥉"}
    rank_label = rank_icons.get(rank, f"#{rank}")

    card_html = (
        f'<div style="background:#ffffff; border-radius:14px; padding:14px;'
        f' border:2px solid {border}; margin-bottom:6px;'
        f' box-shadow:0 2px 8px rgba(0,0,0,0.06);">'
        # 상단: 순위 + 배지
        f'<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">'
        f'<span style="font-size:1.2em;">{rank_label}</span>'
        f'{surge_badge}'
        f'</div>'
        # 티커 + 시장
        f'<div style="font-size:0.72em; color:#94a3b8; margin-bottom:2px;">'
        f'{mkt_badge}{ticker}'
        f'</div>'
        # 종목명
        f'<div style="font-size:0.98em; font-weight:700; color:#1e293b;'
        f' white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">{name}</div>'
        # 가격
        f'<div style="display:flex; justify-content:space-between; align-items:baseline; margin-top:7px;">'
        f'<span style="font-size:1.05em; font-weight:700; color:{chg_color};">{price:,}</span>'
        f'<span style="font-size:0.9em; font-weight:600; color:{chg_color};">{arrow}{abs(change):.2f}%</span>'
        f'</div>'
        # 거래량 비교
        f'<div style="margin-top:7px; background:#f8fafc; border-radius:8px; padding:7px 10px;">'
        f'<div style="font-size:0.7em; color:#64748b;">거래량 비교</div>'
        f'<div style="display:flex; justify-content:space-between; margin-top:3px;">'
        f'<div><div style="font-size:0.65em; color:#94a3b8;">오늘</div>'
        f'<div style="font-size:0.88em; font-weight:700; color:#dc2626;">{vol_today_str}</div></div>'
        f'<div style="font-size:1.1em; color:#94a3b8; padding-top:8px;">→</div>'
        f'<div><div style="font-size:0.65em; color:#94a3b8;">전일</div>'
        f'<div style="font-size:0.88em; font-weight:600; color:#475569;">{vol_prev_str}</div></div>'
        f'</div>'
        f'</div>'
        # 수급
        f'<div style="margin-top:7px; font-size:0.73em;">'
        f'<span style="color:{inst_color}; font-weight:600;">🏛️{inst_str}</span>'
        f'&nbsp;'
        f'<span style="color:{frgn_color}; font-weight:600;">🌍{frgn_str}</span>'
        f'</div>'
        f'</div>'
    )
    st.markdown(card_html, unsafe_allow_html=True)

    # 관심종목 버튼
    wl_tickers = {e["ticker"] for e in get_watchlist()}
    in_wl = ticker in wl_tickers
    if st.button(
        "⭐ 해제" if in_wl else "☆ 관심",
        key=f"vol_surge_wl_{tab_key}_{ticker}",
        use_container_width=True,
        type="secondary",
    ):
        if in_wl:
            remove_from_watchlist(ticker)
        else:
            add_to_watchlist(
                ticker=ticker,
                name=name,
                price=float(price),
                sector=str(row.get("업종", "")),
            )
        st.rerun()


# ─────────────────────────────────────────────────────────────────────
# 테이블 뷰
# ─────────────────────────────────────────────────────────────────────

def _render_surge_table(df: pd.DataFrame):
    """거래량 급증 종목 테이블."""
    rows = []
    for ticker, row in df.iterrows():
        ratio = float(row.get("거래량배수", 1.0))
        surge_label = f"🔥 {ratio:.1f}배" if ratio >= _SURGE_4X_RATIO else f"↑ {ratio:.1f}배"
        rows.append({
            "순위": len(rows) + 1,
            "종목명": row.get("종목명", ticker),
            "티커": ticker,
            "시장": row.get("시장", ""),
            "현재가": f'{int(row.get("종가", 0)):,}',
            "등락률": f'{float(row.get("등락률", 0)):+.2f}%',
            "오늘거래량": f'{int(row.get("거래량", 0)):,}',
            "전일거래량": f'{int(row.get("전일거래량", 0)):,}',
            "거래량증가": surge_label,
            "기관5일(억)": f'{float(row.get("기관합계_5일", 0) or 0) / 1e8:+,.0f}',
            "외인5일(억)": f'{float(row.get("외국인합계_5일", 0) or 0) / 1e8:+,.0f}',
        })
    if rows:
        st.dataframe(pd.DataFrame(rows).set_index("순위"), use_container_width=True)


# ─────────────────────────────────────────────────────────────────────
# 메인 렌더링 함수
# ─────────────────────────────────────────────────────────────────────

@st.fragment
def render_volume_surge_picks(daily_df: pd.DataFrame, date_str: str):
    """
    거래량 급증 발굴 종목 렌더링 (전일대비 거래량 증가율 Top 30).

    4배 이상 급증 시 추세 지속 경향이 있어 발굴 신호로 활용.
    """
    st.markdown("## 📈 거래량 급증 발굴 종목")
    st.caption(
        "전일 대비 거래량이 크게 늘어난 종목을 상위 30개 선별합니다. "
        "🔥 **4배 이상 급증** 종목은 이후 수일간 상승 경향이 있어 주목할 만합니다."
    )

    # 기준 설명 배지
    _render_info_badges()

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    # ── 계산 ─────────────────────────────────────────────────────────
    run_key = "vol_surge_run"
    result_key = "vol_surge_result"

    c1, c2 = st.columns([4, 1])
    with c1:
        if st.button(
            "🔍 거래량 급증 종목 스크리닝 실행",
            key=run_key,
            use_container_width=True,
            type="primary",
        ):
            with st.spinner("📊 전일 거래량 비교 중..."):
                surge_df = compute_volume_surge(daily_df, date_str, top_n=_TOP_N)
            st.session_state[result_key] = surge_df
    with c2:
        if st.button("🔄", key="vol_surge_refresh", help="새로 계산"):
            st.session_state.pop(result_key, None)
            st.rerun()

    surge_df = st.session_state.get(result_key, None)

    if surge_df is None:
        st.info("위 버튼을 눌러 스크리닝을 실행하세요.")
        return

    if surge_df.empty:
        st.warning(
            "전일 스냅샷 데이터가 없어 거래량 증가율을 계산할 수 없습니다. "
            "전일 스냅샷이 저장되면 정상 동작합니다."
        )
        return

    # ── 요약 메트릭 ──────────────────────────────────────────────────
    surge_4x = (surge_df["거래량배수"] >= _SURGE_4X_RATIO).sum()
    surge_2x = (surge_df["거래량배수"] >= _SURGE_2X_RATIO).sum()
    max_ratio = surge_df["거래량배수"].max()
    top_name = surge_df["종목명"].iloc[0] if "종목명" in surge_df.columns else surge_df.index[0]

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("📊 분석 종목", f"{len(surge_df)}개")
    m2.metric("🔥 4배 이상 급증", f"{surge_4x}개")
    m3.metric("📈 2배 이상 급증", f"{surge_2x}개")
    m4.metric("🏆 최고 증가배수", f"{max_ratio:.1f}배", delta=f"{top_name}")

    st.markdown("---")

    # ── 보기 모드 ────────────────────────────────────────────────────
    v1, v2 = st.columns([3, 1])
    with v1:
        st.markdown(
            "<span style='font-size:1.05em; font-weight:700; color:#1e293b;'>"
            f"거래량 증가율 상위 {len(surge_df)}개 종목</span>",
            unsafe_allow_html=True,
        )
    with v2:
        view_mode = st.radio(
            "보기", ["🃏 카드", "📋 테이블"],
            horizontal=True,
            key="vol_surge_view_mode",
            label_visibility="collapsed",
        )

    # ── 4배 이상 급증 섹션 ───────────────────────────────────────────
    hot_df = surge_df[surge_df["거래량배수"] >= _SURGE_4X_RATIO]
    rest_df = surge_df[surge_df["거래량배수"] < _SURGE_4X_RATIO]

    if view_mode == "🃏 카드":
        if not hot_df.empty:
            st.markdown(
                '<div style="background:#fef3c7; border-left:4px solid #f59e0b;'
                ' border-radius:8px; padding:8px 14px; margin-bottom:12px;">'
                '<span style="font-weight:700; color:#92400e;">🔥 4배 이상 급증 — 추세 지속 주목 구간</span>'
                '</div>',
                unsafe_allow_html=True,
            )
            for row_start in range(0, len(hot_df), _COLS_PER_ROW):
                cols = st.columns(_COLS_PER_ROW)
                for j, col in enumerate(cols):
                    idx = row_start + j
                    if idx >= len(hot_df):
                        break
                    ticker = hot_df.index[idx]
                    row = hot_df.iloc[idx]
                    with col:
                        _render_surge_card(ticker, row, idx + 1, "hot")

        if not rest_df.empty:
            if not hot_df.empty:
                st.markdown("#### 📈 2배 미만 급증 종목")
            for row_start in range(0, len(rest_df), _COLS_PER_ROW):
                cols = st.columns(_COLS_PER_ROW)
                for j, col in enumerate(cols):
                    idx = row_start + j
                    if idx >= len(rest_df):
                        break
                    ticker = rest_df.index[idx]
                    row = rest_df.iloc[idx]
                    rank = len(hot_df) + idx + 1
                    with col:
                        _render_surge_card(ticker, row, rank, "rest")
    else:
        _render_surge_table(surge_df)


# ─────────────────────────────────────────────────────────────────────
# 기준 설명 배지
# ─────────────────────────────────────────────────────────────────────

_BADGES = [
    ("🔥 4배 이상", "#f59e0b"),
    ("📈 2배 이상", "#22c55e"),
    ("🏛️ 기관 수급 병행", "#2563eb"),
    ("🌍 외인 수급 병행", "#ea580c"),
    ("전일 스냅샷 기준", "#64748b"),
]


def _render_info_badges():
    badges_html = " ".join([
        f'<span style="background:{c}18; color:{c}; padding:3px 10px;'
        f' border-radius:8px; font-size:0.73em; font-weight:600; margin:2px;">{label}</span>'
        for label, c in _BADGES
    ])
    st.markdown(
        f'<div style="background:#fff; border-left:4px solid #f59e0b;'
        f' border-radius:10px; padding:10px 16px; margin-bottom:12px;'
        f' box-shadow:0 1px 3px rgba(0,0,0,0.05);">'
        f'<div style="font-size:0.83em; color:#475569; margin-bottom:7px;">'
        f'거래량이 4배 이상 급등한 종목은 이후 수일간 상승 경향이 관찰됩니다. '
        f'수급(기관·외국인)과 함께 확인하면 신뢰도가 높아집니다.</div>'
        f'<div style="display:flex; flex-wrap:wrap; gap:4px;">{badges_html}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
