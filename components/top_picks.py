"""
오늘의 발굴 종목 (Top Picks) 컴포넌트
- 수급 상위 카드 표시
- 스크리닝 결과 테이블 (관심종목 체크박스 포함)
"""

from typing import Optional

import pandas as pd
import streamlit as st

from components.watchlist import get_watchlist, add_to_watchlist, remove_from_watchlist


def render_top_cards(daily_df: pd.DataFrame, top_n: int = 5):
    """
    기관+외국인 순매수 합계 TOP N 종목을 카드 형태로 표시.
    """
    st.markdown("## 🔥 수급 TOP 종목")

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    # 수급 합계 컬럼 생성
    supply_cols = [c for c in daily_df.columns if "기관합계_5일" in c or "외국인합계_5일" in c]
    if not supply_cols:
        st.info("수급 데이터가 없습니다.")
        return

    df = daily_df.copy()
    df["수급합계"] = df.get("기관합계_5일", 0) + df.get("외국인합계_5일", 0)
    top = df.nlargest(top_n, "수급합계")
    max_supply = top["수급합계"].max() if not top.empty else 1
    if max_supply == 0:
        max_supply = 1

    cols = st.columns(min(top_n, 5))
    for i, row in enumerate(top.itertuples()):
        ticker = row.Index
        col = cols[i % len(cols)]
        name = getattr(row, '종목명', ticker) if hasattr(row, '종목명') else ticker
        price = getattr(row, '종가', 0) if hasattr(row, '종가') else 0
        change = getattr(row, '등락률', 0) if hasattr(row, '등락률') else 0
        inst = (getattr(row, '기관합계_5일', 0) if hasattr(row, '기관합계_5일') else 0) / 1e8  # 억 원
        frgn = (getattr(row, '외국인합계_5일', 0) if hasattr(row, '외국인합계_5일') else 0) / 1e8
        indv = (getattr(row, '개인_5일', 0) if hasattr(row, '개인_5일') else 0) / 1e8
        sector = getattr(row, '업종', '') if hasattr(row, '업종') else ''
        market = getattr(row, '시장', '') if hasattr(row, '시장') else ''
        supply_total = getattr(row, '수급합계', 0) if hasattr(row, '수급합계') else 0
        strength = min(100, max(0, supply_total / max_supply * 100))

        # 등락률에 따른 색상
        color = "#dc2626" if change > 0 else "#2563eb" if change < 0 else "#94a3b8"
        arrow = "▲" if change > 0 else "▼" if change < 0 else "−"
        rank_num = i + 1

        # 수급 값 안전하게 미리 계산
        inst_sign = "+" if inst > 0 else ""
        frgn_sign = "+" if frgn > 0 else ""
        indv_sign = "+" if indv > 0 else ""
        indv_color = "#16a34a" if indv > 0 else "#dc2626"

        strength_bar = (
            f'<div style="margin-top:8px;">'
            f'<div style="font-size:0.68em; color:#64748b; margin-bottom:2px;">'
            f'수급 강도 {strength:.0f}%</div>'
            f'<div style="background:#e2e8f0; border-radius:6px; height:6px; overflow:hidden;">'
            f'<div style="width:{strength}%; height:100%; background:#7c3aed; border-radius:6px;"></div>'
            f'</div></div>'
        )

        supply_box = (
            f'<div style="margin-top:8px; font-size:0.75em;">'
            f'<span style="color:#2563eb; font-weight:700;">🏛️{inst_sign}{inst:,.1f}억</span>'
            f'&nbsp;&nbsp;'
            f'<span style="color:#ea580c; font-weight:700;">🌍{frgn_sign}{frgn:,.1f}억</span>'
            f'&nbsp;&nbsp;'
            f'<span style="color:{indv_color}; font-weight:700;">👤{indv_sign}{indv:,.1f}억</span>'
            f'</div>'
        )

        mkt_color = "#1d4ed8" if market == "KOSPI" else "#16a34a" if market == "KOSDAQ" else "#94a3b8"
        mkt_badge = (f'<span style="background:{mkt_color}; color:white; padding:1px 6px;'
                     f' border-radius:6px; font-size:0.75em; font-weight:700; margin-left:4px;">'
                     f'{market}</span>') if market else ""

        card_html = (
            f'<div style="background:#ffffff; border-radius:14px; padding:16px;'
            f' margin-bottom:10px; border-left:4px solid #7c3aed;'
            f' border:1px solid #e2e8f0; box-shadow:0 2px 8px rgba(0,0,0,0.06);">'
            f'<div style="font-size:0.72em;">'
            f'<span style="background:#7c3aed; color:white; padding:2px 8px; border-radius:10px; font-weight:700;">#{rank_num}</span>'
            f' <span style="color:#94a3b8;">{ticker}</span>'
            f'{mkt_badge}'
            f' <span style="color:#94a3b8; float:right;">{sector}</span>'
            f'</div>'
            f'<div style="font-size:1.05em; font-weight:bold; color:#1e293b; margin:6px 0 4px;">{name}</div>'
            f'<div style="font-size:1.2em; font-weight:bold; color:{color};">'
            f'{price:,.0f}원 <span style="font-size:0.65em;">{arrow} {abs(change):.2f}%</span></div>'
            f'{strength_bar}'
            f'{supply_box}'
            f'</div>'
        )

        with col:
            st.markdown(card_html, unsafe_allow_html=True)
            # --- 관심종목 버튼 ---
            wl_tickers = {e["ticker"] for e in get_watchlist()}
            in_wl = ticker in wl_tickers
            btn_label = "⭐ 관심 해제" if in_wl else "☆ 관심종목 추가"
            if st.button(btn_label, key=f"wl_card_{ticker}", use_container_width=True):
                if in_wl:
                    remove_from_watchlist(ticker)
                else:
                    add_to_watchlist(
                        ticker=str(ticker),
                        name=str(name),
                        price=float(price),
                        sector=str(sector),
                        market=str(market),
                        source="🔥 발굴:수급TOP",
                    )
                st.rerun()


def render_screened_table(screened_df: pd.DataFrame, top_n: int = 20) -> Optional[str]:
    """
    스크리닝 결과를 테이블로 렌더링하고, 선택한 종목 티커를 반환.
    """
    st.markdown("## 📋 오늘의 발굴 종목")

    if screened_df.empty:
        st.info("스크리닝 조건을 만족하는 종목이 없습니다. 필터를 조정해보세요.")
        return None

    display_cols = []
    col_map = {
        "종목명": "종목명",
        "시장": "시장",
        "종가": "현재가",
        "등락률": "등락률(%)",
        "기관합계_5일": "기관순매수(5일)",
        "외국인합계_5일": "외국인순매수(5일)",
        "개인_5일": "개인순매수(5일)",
        "차트상태": "차트상태",
        "RSI값": "RSI값",
        "RSI상태": "RSI상태",
        "MACD상태": "MACD상태",
        "볼린저상태": "볼린저상태",
        "골든크로스": "골든크로스",
        "거래량급증": "거래량급증",
        "업종": "업종",
        "거래대금": "거래대금(억)",
    }

    df = screened_df.head(top_n).copy()
    df.index.name = "티커"
    df = df.reset_index()

    available_cols = ["티커"] + [k for k in col_map if k in df.columns]
    df_display = df[available_cols].copy()

    # 수급 단위 변환 (억 원)
    for c in ["기관합계_5일", "외국인합계_5일", "개인_5일"]:
        if c in df_display.columns:
            df_display[c] = (df_display[c] / 1e8).round(1)

    if "거래대금" in df_display.columns:
        df_display["거래대금"] = (df_display["거래대금"] / 1e8).round(1)

    rename = {"티커": "티커"}
    rename.update({k: v for k, v in col_map.items() if k in df_display.columns})
    df_display = df_display.rename(columns=rename)

    # 등락률 컬러 표시
    if "등락률(%)" in df_display.columns:
        df_display["등락률(%)"] = df_display["등락률(%)"].round(2)

    # ── 관심종목 체크박스 열 추가 ──
    wl_tickers_before = {e["ticker"] for e in get_watchlist()}
    df_display.insert(0, "⭐", df_display["티커"].isin(wl_tickers_before))

    non_wl_cols = [c for c in df_display.columns if c != "⭐"]

    _EDITOR_KEY = "top_picks_screened_editor"

    edited_df = st.data_editor(
        df_display,
        key=_EDITOR_KEY,
        use_container_width=True,
        hide_index=True,
        height=min(len(df_display) * 38 + 40, 700),
        column_config={
            "⭐": st.column_config.CheckboxColumn(
                "⭐ 관심",
                help="체크하면 관심종목으로 추가됩니다. 추가 당시 현재가를 기준으로 수익률을 추적합니다.",
                width="small",
            ),
            "등락률(%)": st.column_config.NumberColumn(format="%.2f%%"),
            "기관순매수(5일)": st.column_config.NumberColumn(format="%.1f억"),
            "외국인순매수(5일)": st.column_config.NumberColumn(format="%.1f억"),
            "개인순매수(5일)": st.column_config.NumberColumn(format="%.1f억"),
            "거래대금": st.column_config.NumberColumn(format="%.1f억"),
        },
        disabled=non_wl_cols,
    )

    # ── 관심종목 변경 감지 및 반영 ──
    changed = False
    for _, row in edited_df.iterrows():
        ticker = str(row["티커"])
        was_in_wl = ticker in wl_tickers_before
        is_in_wl = bool(row["⭐"])
        if is_in_wl and not was_in_wl:
            stock_rows = df[df["티커"] == ticker]
            if not stock_rows.empty:
                sr = stock_rows.iloc[0]
                success = add_to_watchlist(
                    ticker=ticker,
                    name=str(sr.get("종목명", ticker)),
                    price=float(sr.get("종가", 0)),
                    sector=str(sr.get("업종", "")),
                    market=str(sr.get("시장", "")),
                    source="🔥 발굴:종목테이블",
                )
                if success:
                    changed = True
        elif not is_in_wl and was_in_wl:
            if remove_from_watchlist(ticker):
                changed = True

    if changed:
        # data_editor 위젯 상태를 초기화해서 다음 리런에서 갱신된 관심종목 기반으로 새로 그림
        st.session_state.pop(_EDITOR_KEY, None)
        st.rerun()

    # 종목 선택 (시장 구분 표시)
    ticker_options = df["티커"].tolist()
    name_options = df["종목명"].tolist() if "종목명" in df.columns else ticker_options
    mkt_options = df["시장"].tolist() if "시장" in df.columns else ["" for _ in ticker_options]
    options = [
        f"{t} - [{m}] {n}" if m else f"{t} - {n}"
        for t, n, m in zip(ticker_options, name_options, mkt_options)
    ]

    selected = st.selectbox(
        "🔍 상세 분석할 종목 선택",
        options=["선택하세요..."] + options,
        index=0,
    )

    if selected != "선택하세요...":
        return selected.split(" - ")[0]
    return None
