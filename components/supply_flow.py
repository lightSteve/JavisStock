"""
🏛️ 기관·외국인 수급 분석 컴포넌트
- 수급 대시보드 (기관/외국인 순매수 요약)
- 기관 TOP / 외국인 TOP / 쌍끌이 TOP 랭킹
- 수급 강도 시각화 (바 + 색상 인디케이터)
"""

from typing import Optional
import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go


# ═══════════════════════════════════════════════════════════════════════════
# 메인 진입점
# ═══════════════════════════════════════════════════════════════════════════

def render_supply_flow(daily_df: pd.DataFrame) -> Optional[str]:
    """기관·외국인 수급 분석 메인. 선택한 종목 티커를 반환."""
    st.markdown("## 🏛️ 기관 · 외국인 수급 분석")

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return None

    # 수급 컬럼 확인
    if "기관합계_5일" not in daily_df.columns or "외국인합계_5일" not in daily_df.columns:
        st.warning("수급 데이터가 없습니다. 데이터를 다시 로드해 주세요.")
        return None

    # 일반 종목만 (ETF/ETN 제외)
    df = daily_df.copy()
    if "종목유형" in df.columns:
        df = df[df["종목유형"] == "stock"]

    # ─── 수급 대시보드 ─────────────────────────────────────────
    _render_supply_dashboard(df)

    st.markdown("---")

    # ─── 3개 서브탭 ─────────────────────────────────────────────
    tab_inst, tab_frgn, tab_both = st.tabs([
        "🏛️ 기관 순매수 TOP",
        "🌍 외국인 순매수 TOP",
        "🔥 쌍끌이 (기관+외국인) TOP",
    ])

    selected = None

    with tab_inst:
        r = _render_supply_ranking(
            df, sort_col="기관합계_5일", label="기관",
            color="#2563eb", tab_key="inst",
        )
        if r:
            selected = r

    with tab_frgn:
        r = _render_supply_ranking(
            df, sort_col="외국인합계_5일", label="외국인",
            color="#ea580c", tab_key="frgn",
        )
        if r:
            selected = r

    with tab_both:
        r = _render_supply_ranking(
            df, sort_col="_쌍끌이합계", label="쌍끌이",
            color="#7c3aed", tab_key="both", both_mode=True,
        )
        if r:
            selected = r

    return selected


# ═══════════════════════════════════════════════════════════════════════════
# 수급 대시보드 (상단 요약)
# ═══════════════════════════════════════════════════════════════════════════

def _render_supply_dashboard(df: pd.DataFrame):
    """기관/외국인 수급 핵심 요약 메트릭."""
    inst_col = "기관합계_5일"
    frgn_col = "외국인합계_5일"

    inst_buy = df[df[inst_col] > 0]
    frgn_buy = df[df[frgn_col] > 0]
    both_buy = df[(df[inst_col] > 0) & (df[frgn_col] > 0)]

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("🏛️ 기관 순매수 종목", f"{len(inst_buy):,}개")
    with c2:
        total_inst = inst_buy[inst_col].sum() / 1e8
        st.metric("기관 총 순매수", f"{total_inst:,.0f}억")
    with c3:
        st.metric("🌍 외국인 순매수 종목", f"{len(frgn_buy):,}개")
    with c4:
        total_frgn = frgn_buy[frgn_col].sum() / 1e8
        st.metric("외국인 총 순매수", f"{total_frgn:,.0f}억")
    with c5:
        st.metric("🔥 쌍끌이 종목", f"{len(both_buy):,}개")

    # ─── 기관 vs 외국인 수급 비교 차트 ─────
    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        _render_top_bar_chart(
            df, sort_col=inst_col, label="기관",
            color="#2563eb", n=10,
        )

    with col_chart2:
        _render_top_bar_chart(
            df, sort_col=frgn_col, label="외국인",
            color="#ea580c", n=10,
        )


def _render_top_bar_chart(df: pd.DataFrame, sort_col: str, label: str, color: str, n: int = 10):
    """수평 바 차트로 TOP N 종목 순매수 시각화."""
    top = df.nlargest(n, sort_col).copy()
    top["_display_name"] = top.apply(
        lambda r: r.get("종목명", r.name)[:8], axis=1,
    )
    top["_억"] = (top[sort_col] / 1e8).round(1)

    fig = go.Figure(go.Bar(
        y=top["_display_name"][::-1],
        x=top["_억"][::-1],
        orientation="h",
        marker_color=color,
        text=top["_억"][::-1].apply(lambda x: f"{x:+,.0f}억"),
        textposition="outside",
    ))
    fig.update_layout(
        title=dict(text=f"{label} 순매수 TOP {n}", font=dict(size=14)),
        height=320,
        margin=dict(l=10, r=50, t=35, b=10),
        xaxis_title="순매수 (억원)",
        template="plotly_white",
    )
    st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# 수급 랭킹 (카드 + 테이블)
# ═══════════════════════════════════════════════════════════════════════════

def _render_supply_ranking(
    df: pd.DataFrame,
    sort_col: str,
    label: str,
    color: str,
    tab_key: str,
    both_mode: bool = False,
    top_n: int = 30,
) -> Optional[str]:
    """기관/외국인/쌍끌이 기준 순매수 랭킹 카드 그리드 + 테이블."""

    work = df.copy()

    if both_mode:
        # 쌍끌이: 기관 & 외국인 모두 순매수
        work = work[(work["기관합계_5일"] > 0) & (work["외국인합계_5일"] > 0)].copy()
        work["_쌍끌이합계"] = work["기관합계_5일"] + work["외국인합계_5일"]
        if work.empty:
            st.info("기관·외국인 동시 순매수 종목이 없습니다.")
            return None
    else:
        work = work[work[sort_col] > 0].copy()
        if work.empty:
            st.info(f"{label} 순매수 종목이 없습니다.")
            return None

    work = work.sort_values(sort_col, ascending=False).head(top_n)
    work = work[~work.index.duplicated(keep="first")]

    # 메트릭
    total_count = len(work)
    c1, c2, c3 = st.columns(3)
    c1.metric(f"{label} 순매수 종목", f"{total_count}개")
    avg_buy = work[sort_col].mean() / 1e8
    c2.metric(f"평균 순매수", f"{avg_buy:,.1f}억")
    avg_chg = work["등락률"].mean() if "등락률" in work.columns else 0
    c3.metric(f"평균 등락률", f"{avg_chg:+.2f}%")

    st.markdown("")

    # 보기 모드
    view_mode = st.radio(
        "보기", ["🃏 카드", "📋 테이블"],
        horizontal=True, key=f"supply_view_{tab_key}",
        label_visibility="collapsed",
    )

    sess_key = f"supply_sel_{tab_key}"

    if view_mode == "🃏 카드":
        _render_supply_cards(work, sort_col, label, color, sess_key, tab_key, both_mode)
    else:
        _render_supply_table(work, sort_col, label, sess_key, tab_key, both_mode)

    # 선택된 종목 반환
    sel = st.session_state.get(sess_key)
    if sel:
        return sel
    return None


def _render_supply_cards(
    df: pd.DataFrame,
    sort_col: str,
    label: str,
    color: str,
    sess_key: str,
    tab_key: str,
    both_mode: bool = False,
):
    """수급 랭킹 카드 그리드 (3열)."""
    COLS = 3
    tickers = df.index.tolist()

    # 수급 강도 계산 (max 대비 %)
    max_val = df[sort_col].max()
    if max_val == 0:
        max_val = 1

    for row_start in range(0, len(tickers), COLS):
        cols = st.columns(COLS)
        for j, col in enumerate(cols):
            idx = row_start + j
            if idx >= len(tickers):
                break
            ticker = tickers[idx]
            r = df.iloc[idx]

            name = r.get("종목명", ticker)
            price = int(r.get("종가", 0))
            change = r.get("등락률", 0)
            inst = r.get("기관합계_5일", 0) / 1e8
            frgn = r.get("외국인합계_5일", 0) / 1e8
            indv = r.get("개인_5일", 0) / 1e8 if "개인_5일" in r.index else 0
            sector = r.get("업종", "")

            # 수급 강도 바 (0~100%)
            strength = min(100, max(0, r[sort_col] / max_val * 100))

            # 색상
            chg_color = "#dc2626" if change > 0 else "#2563eb" if change < 0 else "#64748b"
            arrow = "▲" if change > 0 else "▼" if change < 0 else "─"

            is_selected = st.session_state.get(sess_key) == ticker
            border = f"2px solid {color}" if is_selected else "1px solid #e2e8f0"
            bg = "#f8f7ff" if is_selected else "#ffffff"

            # 기관/외국인 각 표시
            inst_sign = "+" if inst > 0 else ""
            frgn_sign = "+" if frgn > 0 else ""

            # 수급 바 색상 (강도에 따라 투명도 조절)
            bar_opacity = 0.2 + (strength / 100) * 0.6

            rank_num = idx + 1

            with col:
                st.markdown(
                    f"""<div style="
                        background:{bg}; border-radius:14px; padding:16px;
                        border:{border}; margin-bottom:6px;
                        box-shadow:0 2px 6px rgba(0,0,0,0.04);
                    ">
                        <div style="display:flex; justify-content:space-between; align-items:center;">
                            <div>
                                <span style="
                                    background:{color}; color:white;
                                    padding:2px 8px; border-radius:10px;
                                    font-size:0.72em; font-weight:700;
                                ">#{rank_num}</span>
                                <span style="font-size:0.72em; color:#94a3b8; margin-left:4px;">{ticker}</span>
                            </div>
                            <span style="font-size:0.68em; color:#94a3b8;">{sector}</span>
                        </div>
                        <div style="font-size:1.05em; font-weight:700; color:#1e293b; margin:6px 0 4px;">
                            {name}
                        </div>
                        <div style="display:flex; justify-content:space-between; align-items:baseline;">
                            <span style="font-size:1.1em; font-weight:700; color:{chg_color};">
                                {price:,}원
                            </span>
                            <span style="font-size:0.88em; font-weight:600; color:{chg_color};">
                                {arrow} {abs(change):.2f}%
                            </span>
                        </div>
                        <div style="margin-top:10px;">
                            <div style="display:flex; justify-content:space-between; font-size:0.72em; color:#64748b; margin-bottom:3px;">
                                <span>수급 강도</span>
                                <span>{strength:.0f}%</span>
                            </div>
                            <div style="background:#e2e8f0; border-radius:6px; height:8px; overflow:hidden;">
                                <div style="
                                    width:{strength}%;
                                    height:100%;
                                    background:linear-gradient(90deg, {color}88, {color});
                                    border-radius:6px;
                                    transition: width 0.3s;
                                "></div>
                            </div>
                        </div>
                        <div style="margin-top:10px; display:flex; gap:6px;">
                            <div style="
                                flex:1; background:#eff6ff; border-radius:8px; padding:8px;
                                text-align:center;
                            ">
                                <div style="font-size:0.68em; color:#64748b;">🏛️ 기관</div>
                                <div style="font-size:0.95em; font-weight:700; color:#2563eb;">
                                    {inst_sign}{inst:,.1f}억
                                </div>
                            </div>
                            <div style="
                                flex:1; background:#fff7ed; border-radius:8px; padding:8px;
                                text-align:center;
                            ">
                                <div style="font-size:0.68em; color:#64748b;">🌍 외국인</div>
                                <div style="font-size:0.95em; font-weight:700; color:#ea580c;">
                                    {frgn_sign}{frgn:,.1f}억
                                </div>
                            </div>
                            <div style="
                                flex:1; background:#f0fdf4; border-radius:8px; padding:8px;
                                text-align:center;
                            ">
                                <div style="font-size:0.68em; color:#64748b;">👤 개인</div>
                                <div style="font-size:0.95em; font-weight:700; color:{'#16a34a' if indv > 0 else '#dc2626'};">
                                    {'+' if indv > 0 else ''}{indv:,.1f}억
                                </div>
                            </div>
                        </div>
                    </div>""",
                    unsafe_allow_html=True,
                )

                st.button(
                    "📈 상세" if not is_selected else "✅ 선택됨",
                    key=f"supply_card_{tab_key}_{ticker}",
                    use_container_width=True,
                    type="primary" if is_selected else "secondary",
                    on_click=lambda t=ticker: st.session_state.update({sess_key: t}),
                )


def _render_supply_table(
    df: pd.DataFrame,
    sort_col: str,
    label: str,
    sess_key: str,
    tab_key: str,
    both_mode: bool = False,
):
    """수급 랭킹 테이블."""
    disp = df.copy()
    disp = disp.reset_index()
    disp = disp.rename(columns={"index": "티커"}) if "티커" not in disp.columns else disp

    show_cols = ["티커", "종목명", "종가", "등락률", "기관합계_5일", "외국인합계_5일"]
    if "개인_5일" in disp.columns:
        show_cols.append("개인_5일")
    if "업종" in disp.columns:
        show_cols.append("업종")

    show_cols = [c for c in show_cols if c in disp.columns]
    table = disp[show_cols].copy()

    # 단위 변환
    for c in ["기관합계_5일", "외국인합계_5일", "개인_5일"]:
        if c in table.columns:
            table[c] = (table[c] / 1e8).round(1)

    table = table.rename(columns={
        "종가": "현재가",
        "등락률": "등락률(%)",
        "기관합계_5일": "기관(억)",
        "외국인합계_5일": "외국인(억)",
        "개인_5일": "개인(억)",
    })

    st.dataframe(
        table,
        use_container_width=True,
        hide_index=True,
        height=min(len(table) * 38 + 50, 700),
        column_config={
            "등락률(%)": st.column_config.NumberColumn(format="%+.2f%%"),
            "기관(억)": st.column_config.NumberColumn(format="%+.1f"),
            "외국인(억)": st.column_config.NumberColumn(format="%+.1f"),
            "개인(억)": st.column_config.NumberColumn(format="%+.1f"),
        },
    )

    # 종목 선택
    tickers = df.index.tolist()
    names = [df.loc[t, "종목명"] if "종목명" in df.columns else t for t in tickers]
    options = [f"{t} - {n}" for t, n in zip(tickers, names)]

    def _on_select():
        val = st.session_state.get(f"supply_tbl_{tab_key}", "선택하세요...")
        if val != "선택하세요...":
            st.session_state[sess_key] = val.split(" - ")[0].strip()

    st.selectbox(
        "🔍 종목 선택 → 상세 보기",
        ["선택하세요..."] + options,
        index=0,
        key=f"supply_tbl_{tab_key}",
        on_change=_on_select,
    )
