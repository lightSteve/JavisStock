"""
🚨 Panel 4: Event-Driven Radar
(유형 B & D 공략: 바이오 낙폭 및 속보)
- 바이오 루머 급락 스캐너
- 해명 공시 연동 (급락 후 회복 베팅 구간)
- 단독 뉴스 스파이크 (키워드 + 체결 강도)
"""

import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go

from data.fetcher import (
    detect_sharp_drop_stocks,
    get_stock_news_list,
    get_stock_ohlcv_history,
    get_investor_trend_individual,
    detect_volume_spike_stocks,
)


# 바이오 관련 업종/키워드
_BIO_KEYWORDS = [
    "바이오", "제약", "의약", "헬스", "셀", "진단", "신약",
    "임상", "의료", "생명", "줄기세포", "항체", "백신", "유전자",
]

# 뉴스 스파이크 감지 키워드
_NEWS_SPIKE_KEYWORDS = [
    "단독", "특징주", "급등", "상한가", "테마", "수주", "계약",
    "FDA", "승인", "허가", "합병", "인수", "MOU", "공급",
    "정부", "정책", "대통령", "국회", "긴급",
]


# ═══════════════════════════════════════════════════════════════════════════
# 메인 진입점
# ═══════════════════════════════════════════════════════════════════════════

def render_event_radar(daily_df: pd.DataFrame, date_str: str):
    """Panel 4: Event-Driven Radar 렌더링."""

    import datetime as _dt
    st.markdown("## 🚨 Event-Driven Radar")
    st.caption("바이오 급락 스캔 · 회복 베팅 구간 · 뉴스 스파이크")

    # 마지막 갱신 시간 표시
    last_update_key = "event_radar_last_update"
    if last_update_key not in st.session_state:
        st.session_state[last_update_key] = None

    # 데이터로드 & 분석시작 버튼
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("🔄 데이터로드 & 분석시작", key="event_radar_reload"):
            st.session_state[last_update_key] = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.experimental_rerun()
    with col2:
        last_time = st.session_state[last_update_key]
        if last_time:
            st.info(f"마지막 갱신: {last_time}")
        else:
            st.info("아직 데이터가 갱신되지 않았습니다.")

    if daily_df.empty:
        st.info("데이터가 없습니다.")
        return

    tab_bio, tab_recover, tab_news = st.tabs([
        "🧬 바이오 급락 스캐너",
        "📈 회복 베팅 구간",
        "📰 뉴스 스파이크",
    ])

    with tab_bio:
        _render_bio_crash_scanner(daily_df, date_str)

    with tab_recover:
        _render_recovery_zone(daily_df, date_str)

    with tab_news:
        _render_news_spike(daily_df, date_str)


# ═══════════════════════════════════════════════════════════════════════════
# 1) 바이오 루머 급락 스캐너
# ═══════════════════════════════════════════════════════════════════════════

def _is_bio_stock(row: pd.Series) -> bool:
    """바이오/제약 관련 종목인지 판단."""
    sector = str(row.get("업종", ""))
    name = str(row.get("종목명", ""))
    text = sector + " " + name

    return any(kw in text for kw in _BIO_KEYWORDS)


def _render_bio_crash_scanner(daily_df: pd.DataFrame, date_str: str):
    """바이오 종목 급락 스캐너."""

    from data.price_cache import price_cache
    st.markdown("### 🧬 바이오 루머 급락 스캐너")
    st.caption("중요 이벤트를 앞둔 바이오 주식이 출처 불명의 루머로 -10%~-20% 급락하는 패턴")

    # 바이오 종목 필터
    bio_mask = daily_df.apply(_is_bio_stock, axis=1)
    bio_df = daily_df[bio_mask]

    if bio_df.empty:
        st.info("바이오/제약 관련 종목 데이터가 없습니다.")
        return

    # 급락 바이오 종목 (–5% 이하)
    crash_bio = bio_df[bio_df["등락률"] <= -5].sort_values("등락률", ascending=True)
    # 현재가 실시간 갱신
    tickers = crash_bio.index.tolist()
    if tickers:
        price_cache.ensure_fresh(tickers)
        price_cache.apply_to_dataframe(daily_df, tickers)

    # 메트릭
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("🧬 바이오 전체", f"{len(bio_df)}개")
    with c2:
        severe_crash = bio_df[bio_df["등락률"] <= -10]
        st.metric("🔴 급락(-10%↓)", f"{len(severe_crash)}개")
    with c3:
        st.metric("🟡 하락(-5%~-10%)", f"{len(crash_bio) - len(severe_crash)}개")

    if crash_bio.empty:
        st.success("✅ 현재 급락 중인 바이오 종목이 없습니다.")
        return

    # 급락 종목 리스트
    for ticker, row in crash_bio.iterrows():
        name = row.get("종목명", ticker)
        change = row.get("등락률", 0)
        price = row.get("종가", 0)
        tv = row.get("거래대금", 0) / 1e8
        sector = row.get("업종", "")

        # 급락 심각도
        if change <= -20:
            severity = "🔴 심각"
            sev_color = "#991b1b"
        elif change <= -10:
            severity = "🟠 경고"
            sev_color = "#ea580c"
        else:
            severity = "🟡 주의"
            sev_color = "#f59e0b"

        st.markdown(
            f'<div style="background:#fff; border-radius:12px; padding:14px; margin-bottom:8px; '
            f'border-left:4px solid {sev_color}; box-shadow:0 1px 4px rgba(0,0,0,0.04);">'
            f'<div style="display:flex; justify-content:space-between; align-items:center; '
            f'flex-wrap:wrap; gap:8px;">'
            f'<div style="flex:1; min-width:140px;">'
            f'<span style="background:{sev_color}; color:#fff; padding:2px 8px; '
            f'border-radius:6px; font-size:0.65em; font-weight:700;">{severity}</span>'
            f'<div style="font-weight:700; font-size:1.05em; color:#1e293b; margin-top:4px;">{name}'
            f'<span style="font-size:0.72em; color:#94a3b8; margin-left:6px;">{ticker}</span></div>'
            f'<div style="font-size:0.78em; color:#64748b;">{sector}</div></div>'
            f'<div style="text-align:center;">'
            f'<div style="font-size:1.3em; font-weight:800; color:#2563eb;">{change:.2f}%</div>'
            f'<div style="font-size:0.78em; color:#64748b;">{price:,.0f}원</div></div>'
            f'<div style="text-align:center;">'
            f'<div style="font-size:0.72em; color:#94a3b8;">거래대금</div>'
            f'<div style="font-size:0.92em; font-weight:600; color:#1e293b;">{tv:,.0f}억</div></div>'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # 관련 뉴스 체크
    st.markdown("#### 📰 급락 종목 관련 뉴스")
    for ticker, row in crash_bio.head(3).iterrows():
        name = row.get("종목명", ticker)
        news = get_stock_news_list(ticker, count=5)
        if news:
            with st.expander(f"📰 {name} 최신 뉴스 ({len(news)}건)"):
                for article in news:
                    title = article.get("title", "")
                    date = article.get("date", "")
                    source = article.get("source", "")
                    # 키워드 하이라이트
                    is_rumor = any(kw in title for kw in ["루머", "소문", "의혹", "논란", "급락", "하락"])
                    is_denial = any(kw in title for kw in ["해명", "부인", "반박", "사실무근", "소송", "법적"])
                    badge = ""
                    if is_denial:
                        badge = ('<span style="background:#dcfce7; color:#16a34a; padding:1px 6px; '
                                 'border-radius:4px; font-size:0.65em; font-weight:700; margin-right:4px;">'
                                 '✅ 해명/부인</span>')
                    elif is_rumor:
                        badge = ('<span style="background:#fee2e2; color:#991b1b; padding:1px 6px; '
                                 'border-radius:4px; font-size:0.65em; font-weight:700; margin-right:4px;">'
                                 '⚠️ 루머/의혹</span>')

                    st.markdown(
                        f'<div style="padding:6px 0; border-bottom:1px solid #f1f5f9; font-size:0.85em;">'
                        f'{badge}<span style="color:#1e293b;">{title}</span>'
                        f'<span style="color:#94a3b8; font-size:0.8em; margin-left:6px;">'
                        f'{source} · {date}</span></div>',
                        unsafe_allow_html=True,
                    )


# ═══════════════════════════════════════════════════════════════════════════
# 2) 회복 베팅 구간
# ═══════════════════════════════════════════════════════════════════════════

def _render_recovery_zone(daily_df: pd.DataFrame, date_str: str):
    """급락 후 회복 가능성 있는 종목."""

    from data.price_cache import price_cache
    st.markdown("### 📈 회복 베팅 구간")
    st.caption("급락 후 해명 공시/법적 대응/기관 매수 전환 시 회복 베팅 포인트")

    # 급락 종목 감지 (전 업종)
    drop_df = detect_sharp_drop_stocks(daily_df, threshold=-10.0)
    if drop_df.empty:
        drop_df = detect_sharp_drop_stocks(daily_df, threshold=-5.0)

    if drop_df.empty:
        st.success("✅ 현재 급락 종목이 없습니다.")
        return

    # 현재가 실시간 갱신
    tickers = drop_df.index.tolist()
    if tickers:
        price_cache.ensure_fresh(tickers)
        price_cache.apply_to_dataframe(daily_df, tickers)

    st.markdown(f"**📉 급락 종목: {len(drop_df)}건**")

    recovery_candidates = []

    for ticker, row in drop_df.head(10).iterrows():
        name = row.get("종목명", ticker)
        change = row.get("등락률", 0)
        price = row.get("종가", 0)
        sector = row.get("업종", "")

        # 뉴스 체크 (해명/부인 공시 탐색)
        news = get_stock_news_list(ticker, count=5)

        has_denial = False
        denial_title = ""
        for article in news:
            title = article.get("title", "")
            if any(kw in title for kw in ["해명", "부인", "반박", "사실무근", "소송", "법적", "정정"]):
                has_denial = True
                denial_title = title
                break

        # 기관 수급 체크
        inst_5d = row.get("기관합계_5일", 0) if "기관합계_5일" in row.index else 0
        frgn_5d = row.get("외국인합계_5일", 0) if "외국인합계_5일" in row.index else 0
        has_inst_support = inst_5d > 0 or frgn_5d > 0

        # 회복 시그널 점수
        recovery_score = 0
        signals = []
        if has_denial:
            recovery_score += 40
            signals.append("✅ 해명/부인 공시 확인")
        if has_inst_support:
            recovery_score += 30
            signals.append(f"🏛️ 기관/외국인 수급 양호")
        if abs(change) >= 15:
            recovery_score += 20
            signals.append("📉 과매도 구간 진입")
        if row.get("거래대금", 0) / 1e8 >= 100:
            recovery_score += 10
            signals.append("📊 높은 거래대금 (유동성)")

        recovery_candidates.append({
            "ticker": ticker,
            "name": name,
            "change": change,
            "price": price,
            "sector": sector,
            "has_denial": has_denial,
            "denial_title": denial_title,
            "has_inst_support": has_inst_support,
            "inst_5d": inst_5d,
            "frgn_5d": frgn_5d,
            "recovery_score": recovery_score,
            "signals": signals,
            "news": news,
        })

    # 회복 점수 기준 정렬
    recovery_candidates.sort(key=lambda x: x["recovery_score"], reverse=True)

    for res in recovery_candidates:
        score = res["recovery_score"]
        chg = res["change"]

        # 상태 색상
        if score >= 60:
            zone = "🟢 회복 유력"
            zone_color = "#16a34a"
            zone_bg = "#dcfce7"
        elif score >= 30:
            zone = "🟡 회복 가능"
            zone_color = "#f59e0b"
            zone_bg = "#fef3c7"
        else:
            zone = "🔴 추가 관찰"
            zone_color = "#dc2626"
            zone_bg = "#fee2e2"

        inst_억 = res["inst_5d"] / 1e8
        frgn_억 = res["frgn_5d"] / 1e8

        signals_html = " ".join(
            f'<span style="background:#f1f5f9; padding:2px 6px; border-radius:4px; '
            f'font-size:0.68em; margin-right:3px;">{s}</span>'
            for s in res["signals"]
        )

        st.markdown(
            f'<div style="background:#fff; border-radius:12px; padding:16px; margin-bottom:10px; '
            f'border:1px solid #e2e8f0; box-shadow:0 2px 6px rgba(0,0,0,0.04);">'
            f'<div style="display:flex; justify-content:space-between; align-items:center; '
            f'flex-wrap:wrap; gap:8px; margin-bottom:8px;">'
            # 종목 정보
            f'<div>'
            f'<div style="font-weight:700; font-size:1.05em; color:#1e293b;">{res["name"]}'
            f'<span style="font-size:0.72em; color:#94a3b8; margin-left:6px;">{res["ticker"]}</span></div>'
            f'<div style="font-size:0.78em; color:#64748b;">{res["sector"]}</div></div>'
            # 등락률
            f'<div style="text-align:center;">'
            f'<div style="font-size:1.3em; font-weight:800; color:#2563eb;">{chg:.1f}%</div>'
            f'<div style="font-size:0.78em; color:#64748b;">{res["price"]:,.0f}원</div></div>'
            # 회복 점수
            f'<div style="text-align:center;">'
            f'<span style="background:{zone_bg}; color:{zone_color}; padding:4px 12px; '
            f'border-radius:10px; font-size:0.82em; font-weight:700;">{zone}</span>'
            f'<div style="font-size:0.68em; color:#94a3b8; margin-top:3px;">회복 점수 {score}</div></div>'
            f'</div>'
            # 시그널
            f'<div style="margin-top:6px;">{signals_html}</div>'
            # 수급
            f'<div style="margin-top:8px; font-size:0.78em;">'
            f'<span style="color:#2563eb;">🏛️ 기관 {inst_억:+,.0f}억</span>'
            f'<span style="color:#ea580c; margin-left:10px;">🌍 외국인 {frgn_억:+,.0f}억</span></div>'
            # 해명 뉴스
            f'{"<div style=margin-top:6px;background:#dcfce7;padding:6px 10px;border-radius:6px;font-size:0.78em;color:#16a34a;font-weight:600;>📰 " + res["denial_title"] + "</div>" if res["has_denial"] else ""}'
            f'</div>',
            unsafe_allow_html=True,
        )


# ═══════════════════════════════════════════════════════════════════════════
# 3) 단독 뉴스 스파이크
# ═══════════════════════════════════════════════════════════════════════════

def _render_news_spike(daily_df: pd.DataFrame, date_str: str):
    """단독 기사 출현 + 체결 강도 급증 종목."""

    from data.price_cache import price_cache
    st.markdown("### 📰 단독 뉴스 스파이크")
    st.caption("특정 키워드 포함 단독 기사 출현 직후, 거래량/체결 강도가 급증하는 종목")

    # 급등 + 높은 거래대금 종목
    spike_df = detect_volume_spike_stocks(daily_df, min_change=3.0)
    if spike_df.empty:
        st.info("거래대금 급등 종목이 없습니다.")
        return

    # 현재가 실시간 갱신
    tickers = spike_df.index.tolist()
    if tickers:
        price_cache.ensure_fresh(tickers)
        price_cache.apply_to_dataframe(daily_df, tickers)

    # 상위 종목 뉴스 체크
    news_spikes = []

    progress = st.progress(0, text="뉴스 스파이크 분석 중...")
    check_tickers = spike_df.head(15)

    for idx, (ticker, row) in enumerate(check_tickers.iterrows()):
        progress.progress((idx + 1) / len(check_tickers), text=f"뉴스 확인: {row.get('종목명', ticker)}")

        news = get_stock_news_list(ticker, count=5)
        if not news:
            continue

        # 스파이크 키워드 매칭
        matching_articles = []
        matched_keywords = set()
        for article in news:
            title = article.get("title", "")
            for kw in _NEWS_SPIKE_KEYWORDS:
                if kw in title:
                    matching_articles.append(article)
                    matched_keywords.add(kw)
                    break

        if matching_articles:
            news_spikes.append({
                "ticker": ticker,
                "name": row.get("종목명", ticker),
                "change": row.get("등락률", 0),
                "price": row.get("종가", 0),
                "tv": row.get("거래대금", 0),
                "articles": matching_articles,
                "keywords": list(matched_keywords),
                "article_count": len(matching_articles),
                "sector": row.get("업종", ""),
            })

    progress.empty()

    if not news_spikes:
        st.info("📭 현재 뉴스 스파이크가 감지된 종목이 없습니다.")
        return

    # 키워드 매칭 수 + 등락률 기준 정렬
    news_spikes.sort(key=lambda x: (-x["article_count"], -x["change"]))

    st.markdown(f"**🔥 뉴스 스파이크 감지: {len(news_spikes)}건**")

    for res in news_spikes[:10]:
        chg = res["change"]
        chg_color = "#dc2626" if chg > 0 else "#2563eb"
        tv_억 = res["tv"] / 1e8
        kw_html = " ".join(
            f'<span style="background:#ede9fe; color:#7c3aed; padding:1px 6px; '
            f'border-radius:4px; font-size:0.65em; font-weight:600;">#{kw}</span>'
            for kw in res["keywords"]
        )

        st.markdown(
            f'<div style="background:#fff; border-radius:12px; padding:14px; margin-bottom:8px; '
            f'border:1px solid #e2e8f0; box-shadow:0 2px 6px rgba(0,0,0,0.04);">'
            # 헤더
            f'<div style="display:flex; justify-content:space-between; align-items:center; '
            f'flex-wrap:wrap; gap:8px; margin-bottom:6px;">'
            f'<div>'
            f'<span style="background:#ef4444; color:#fff; padding:2px 8px; '
            f'border-radius:6px; font-size:0.68em; font-weight:700;">'
            f'📰 뉴스 스파이크</span></div>'
            f'<div style="font-weight:800; color:{chg_color}; font-size:1.15em;">'
            f'{"+" if chg > 0 else ""}{chg:.2f}%</div></div>'
            # 종목 정보
            f'<div style="font-weight:700; font-size:1.05em; color:#1e293b;">{res["name"]}'
            f'<span style="font-size:0.72em; color:#94a3b8; margin-left:6px;">{res["ticker"]}</span></div>'
            f'<div style="font-size:0.78em; color:#64748b; margin-top:2px;">'
            f'{res["sector"]} · 거래대금 {tv_억:,.0f}억</div>'
            # 키워드
            f'<div style="margin-top:6px;">{kw_html}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # 뉴스 기사 목록 (확장)
        with st.expander(f"📋 {res['name']} 관련 뉴스 ({res['article_count']}건)"):
            for article in res["articles"]:
                title = article.get("title", "")
                date = article.get("date", "")
                source = article.get("source", "")
                st.markdown(
                    f'<div style="padding:5px 0; border-bottom:1px solid #f1f5f9; font-size:0.85em;">'
                    f'<span style="color:#1e293b;">{title}</span>'
                    f'<span style="color:#94a3b8; font-size:0.8em; margin-left:6px;">'
                    f'{source} · {date}</span></div>',
                    unsafe_allow_html=True,
                )
