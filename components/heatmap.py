"""
섹터 히트맵 컴포넌트 (Sector Heatmap)
- Plotly Treemap으로 섹터별 등락률 시각화
- 기관/외국인 자금 유입 TOP 섹터 하이라이트
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


def render_sector_heatmap(daily_df: pd.DataFrame):
    """
    섹터별 평균 등락률 Treemap + 수급 TOP 섹터 하이라이트.
    daily_df: build_daily_dataset() 결과 (업종, 등락률, 수급 컬럼 포함)
    """

    import datetime as _dt
    st.markdown("## 🗺️ 섹터 히트맵")

    # 마지막 갱신 시간 표시
    last_update_key = "sector_heatmap_last_update"
    if last_update_key not in st.session_state:
        st.session_state[last_update_key] = None

    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("🔄 데이터로드 & 분석시작", key="sector_heatmap_reload"):
            st.session_state[last_update_key] = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.experimental_rerun()
    with col2:
        last_time = st.session_state[last_update_key]
        if last_time:
            st.info(f"마지막 갱신: {last_time}")
        else:
            st.info("아직 데이터가 갱신되지 않았습니다.")

    if daily_df.empty or "업종" not in daily_df.columns:
        st.warning("섹터 데이터가 없습니다. 데이터를 먼저 로드해주세요.")
        return

    # 업종 미분류 종목 제외
    df = daily_df.dropna(subset=["업종"]).copy()
    df = df[df["업종"].str.strip() != ""]

    if df.empty:
        st.warning("업종 정보가 있는 종목이 없습니다.")
        return

    # --- 섹터별 집계 ---
    agg_dict = {
        "평균등락률": ("등락률", "mean"),
        "종목수": ("등락률", "count"),
        "총거래대금": ("거래대금", "sum"),
    }
    if "시가총액" in df.columns:
        agg_dict["총시가총액"] = ("시가총액", "sum")

    sector_agg = df.groupby("업종").agg(**agg_dict).reset_index()

    # 크기 기준: 시가총액 우선, 없으면 거래대금 사용
    if "총시가총액" in sector_agg.columns:
        sector_agg["총시가총액"] = sector_agg["총시가총액"].fillna(0)
        # 시가총액이 0인 섹터가 많으면 거래대금으로 fallback
        valid_mktcap = (sector_agg["총시가총액"] > 0).sum()
        size_col = "총시가총액" if valid_mktcap >= len(sector_agg) * 0.5 else "총거래대금"
    else:
        size_col = "총거래대금"

    # 크기 값이 0이면 최솟값(1)으로 채워 treemap 오류 방지
    sector_agg[size_col] = sector_agg[size_col].clip(lower=1)

    # 수급 집계 (있는 경우)
    if "기관합계_5일" in df.columns:
        supply_agg = df.groupby("업종").agg(
            기관순매수합=("기관합계_5일", "sum"),
            외국인순매수합=("외국인합계_5일", "sum"),
        ).reset_index()
        sector_agg = sector_agg.merge(supply_agg, on="업종", how="left")
        sector_agg["기관외국인합"] = (
            sector_agg["기관순매수합"].fillna(0) + sector_agg["외국인순매수합"].fillna(0)
        )

    # 등락률 기반 색상 범위 (최소 ±2%)
    max_abs = max(
        abs(sector_agg["평균등락률"].min()),
        abs(sector_agg["평균등락률"].max()),
        2.0,
    )

    # 시가총액을 조 단위로 표시
    size_label = "시가총액" if size_col == "총시가총액" else "거래대금"
    sector_agg["_size_display"] = (sector_agg[size_col] / 1e12).round(2)  # 조 원

    # --- Treemap ---
    fig = px.treemap(
        sector_agg,
        path=["업종"],
        values=size_col,
        color="평균등락률",
        color_continuous_scale="RdYlGn",
        range_color=[-max_abs, max_abs],
        custom_data=["평균등락률", "종목수", "_size_display"],
        title=f"섹터별 등락률 히트맵  (크기={size_label} 비중 · 색상=평균 등락률)",
    )
    fig.update_traces(
        texttemplate="<b>%{label}</b><br>%{customdata[0]:+.2f}%",
        textfont=dict(size=13),
        hovertemplate=(
            "<b>%{label}</b><br>"
            "평균 등락률: %{customdata[0]:+.2f}%<br>"
            "종목 수: %{customdata[1]}개<br>"
            f"{size_label}: %{{customdata[2]:,.1f}}조원<br>"
            "<extra></extra>"
        ),
    )
    fig.update_layout(
        height=520,
        margin=dict(t=50, l=10, r=10, b=10),
        coloraxis_colorbar=dict(title="등락률(%)"),
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- 기관/외국인 자금 TOP 섹터 ---
    if "기관외국인합" in sector_agg.columns:
        st.markdown("### 🏆 기관 + 외국인 순매수 TOP 5 섹터")
        top_sectors = sector_agg.nlargest(5, "기관외국인합")[
            ["업종", "기관순매수합", "외국인순매수합", "기관외국인합", "평균등락률"]
        ].reset_index(drop=True)
        top_sectors.index = top_sectors.index + 1

        # 단위를 억 원으로 변환
        for c in ["기관순매수합", "외국인순매수합", "기관외국인합"]:
            top_sectors[c] = (top_sectors[c] / 1e8).round(1)
        top_sectors.columns = ["업종", "기관(억)", "외국인(억)", "합계(억)", "평균등락률(%)"]
        top_sectors["평균등락률(%)"] = top_sectors["평균등락률(%)"].round(2)

        st.dataframe(
            top_sectors,
            use_container_width=True,
            hide_index=False,
        )


def render_sector_bar_chart(daily_df: pd.DataFrame):
    """섹터별 수급 막대 차트."""
    if daily_df.empty or "업종" not in daily_df.columns:
        return

    if "기관합계_5일" not in daily_df.columns:
        return

    sector_supply = daily_df.groupby("업종").agg(
        기관합계=("기관합계_5일", "sum"),
        외국인합계=("외국인합계_5일", "sum"),
    ).reset_index()
    sector_supply["합계"] = sector_supply["기관합계"] + sector_supply["외국인합계"]
    sector_supply = sector_supply.sort_values("합계", ascending=True).tail(15)

    # 억 원 단위 변환
    for c in ["기관합계", "외국인합계"]:
        sector_supply[c] = (sector_supply[c] / 1e8).round(1)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=sector_supply["업종"], x=sector_supply["기관합계"],
        name="기관(억)", orientation="h",
        marker_color="#2563eb",
    ))
    fig.add_trace(go.Bar(
        y=sector_supply["업종"], x=sector_supply["외국인합계"],
        name="외국인(억)", orientation="h",
        marker_color="#ea580c",
    ))
    fig.update_layout(
        barmode="group",
        title="섹터별 5일 누적 수급 TOP 15 (억 원)",
        xaxis_title="순매수 (억 원)",
        yaxis_title="",
        height=500,
        margin=dict(l=10, r=10),
    )
    st.plotly_chart(fig, use_container_width=True)
