"""
🇺🇸 미국 주요 지수 배너 (나스닥·S&P500·다우존스)
- 전일 대비 등락 표시
- 전체 분위기 기반 국내 시장 대응 시그널 제공
"""

import streamlit as st
from data.fetcher import get_us_index_summary


def _signal_config(avg_pct: float) -> dict:
    """평균 등락률을 기반으로 매매 시그널 설정 반환."""
    if avg_pct >= 1.5:
        return {
            "emoji": "🚀",
            "label": "강세 마감 — 적극 매매 가능",
            "desc": "미국 3대 지수가 모두 강하게 상승했습니다. 국내 시장도 갭 상승·강세 출발 가능성이 높습니다.",
            "bg": "linear-gradient(135deg, #166534 0%, #15803d 100%)",
            "badge_bg": "#bbf7d0",
            "badge_color": "#14532d",
        }
    elif avg_pct >= 0.3:
        return {
            "emoji": "📈",
            "label": "소폭 상승 마감 — 정상 매매",
            "desc": "미국 시장이 소폭 상승 마감했습니다. 국내 시장은 안정적 흐름이 예상됩니다.",
            "bg": "linear-gradient(135deg, #1e3a5f 0%, #1d4ed8 100%)",
            "badge_bg": "#bfdbfe",
            "badge_color": "#1e3a8a",
        }
    elif avg_pct >= -0.3:
        return {
            "emoji": "😐",
            "label": "보합 마감 — 방향성 주시",
            "desc": "미국 시장이 보합권에서 마감했습니다. 섹터·수급 중심으로 개별 종목에 집중하세요.",
            "bg": "linear-gradient(135deg, #374151 0%, #4b5563 100%)",
            "badge_bg": "#e5e7eb",
            "badge_color": "#111827",
        }
    elif avg_pct >= -1.5:
        return {
            "emoji": "⚠️",
            "label": "소폭 하락 마감 — 매매 신중",
            "desc": "미국 시장이 하락 마감했습니다. 국내 시장 약세 출발이 예상되므로 신규 진입을 줄이세요.",
            "bg": "linear-gradient(135deg, #92400e 0%, #d97706 100%)",
            "badge_bg": "#fef3c7",
            "badge_color": "#78350f",
        }
    else:
        return {
            "emoji": "🛑",
            "label": "급락 마감 — 매매 자제",
            "desc": "미국 지수가 급락했습니다. 국내 시장도 급락 출발 가능성이 큽니다. 오늘은 관망을 권장합니다.",
            "bg": "linear-gradient(135deg, #7f1d1d 0%, #dc2626 100%)",
            "badge_bg": "#fee2e2",
            "badge_color": "#7f1d1d",
        }


def render_us_market_banner():
    """미국 3대 지수 배너를 렌더링한다."""
    indices = get_us_index_summary()

    if not indices:
        st.info("🇺🇸 미국 지수 데이터를 불러오는 중입니다...")
        return

    # 평균 등락률 계산
    avg_pct = sum(i["pct"] for i in indices) / len(indices)
    cfg = _signal_config(avg_pct)

    # ── 헤더 배너 ──
    date_str = ""
    if indices and indices[0].get("date"):
        from datetime import datetime
        try:
            d = datetime.strptime(indices[0]["date"], "%Y-%m-%d")
            date_str = d.strftime("%m/%d") + " 마감"
        except Exception:
            date_str = indices[0]["date"]

    header_html = (
        f'<div style="background:{cfg["bg"]}; color:#fff; border-radius:16px; '
        f'padding:16px 20px 14px; margin-bottom:10px; '
        f'box-shadow:0 4px 14px rgba(0,0,0,0.18);">'
        # 타이틀 행
        f'<div style="display:flex; justify-content:space-between; align-items:center; '
        f'margin-bottom:12px;">'
        f'<div style="font-size:1.1em; font-weight:800;">'
        f'🇺🇸 미국 증시 {date_str}</div>'
        f'<div style="background:{cfg["badge_bg"]}; color:{cfg["badge_color"]}; '
        f'border-radius:20px; padding:4px 14px; font-size:0.82em; font-weight:700;">'
        f'{cfg["emoji"]} {cfg["label"]}</div>'
        f'</div>'
        # 지수 카드 행
        f'<div style="display:grid; grid-template-columns:repeat(3,1fr); gap:10px;">'
    )

    for idx in indices:
        pct = idx["pct"]
        close = idx["close"]
        change = idx["change"]

        arrow = "▲" if pct >= 0 else "▼"
        pct_color = "#4ade80" if pct >= 0 else "#f87171"
        sign = "+" if change >= 0 else ""

        # 지수별 이모지
        name_emoji = {"NASDAQ": "💻", "S&P500": "🏦", "DOW": "🏭"}.get(idx["name"], "📊")

        header_html += (
            f'<div style="background:rgba(255,255,255,0.12); border-radius:10px; '
            f'padding:10px 12px; text-align:center;">'
            f'<div style="font-size:0.75em; opacity:0.85; margin-bottom:4px;">'
            f'{name_emoji} {idx["name"]}</div>'
            f'<div style="font-size:1.15em; font-weight:800;">'
            f'{close:,.2f}</div>'
            f'<div style="font-size:0.8em; color:{pct_color}; font-weight:700; margin-top:2px;">'
            f'{arrow} {sign}{pct:.2f}% ({sign}{change:,.2f})</div>'
            f'</div>'
        )

    header_html += (
        f'</div>'
        # 분석 코멘트
        f'<div style="margin-top:12px; font-size:0.83em; opacity:0.9; '
        f'border-top:1px solid rgba(255,255,255,0.2); padding-top:10px;">'
        f'💡 {cfg["desc"]}'
        f'</div>'
        f'</div>'
    )

    st.markdown(header_html, unsafe_allow_html=True)
