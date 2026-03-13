"""
시장 국면(Market Regime) & 비중 조절 로직

- calc_market_regime()       : 시장 상태 레벨 산출 (냉각장/보통/과열)
- suggest_position_size()    : 레짐 기반 권장 포지션 비중
- calc_pnl_based_sizing()    : 수익/손실 기반 비중 조절 (리버패스 스타일)
- calc_20day_trading_value() : 20일 평균 거래대금 대비 비율
"""

from typing import Dict, List, Tuple
import pandas as pd
import numpy as np


# ═══════════════════════════════════════════════════════════════════════════
# 시장 레짐 계산
# ═══════════════════════════════════════════════════════════════════════════

def calc_market_regime(daily_df: pd.DataFrame) -> Dict:
    """시장 상태를 다차원 지표로 평가하고 레짐 레이블을 반환.

    Args:
        daily_df: 당일 전종목 데이터 (cols: 거래대금, 등락률, 시장, ...)

    Returns:
        dict with keys:
            level       : int (1=냉각장, 2=보통, 3=과열)
            label       : str ("냉각장" | "보통" | "과열(호황)")
            color       : str (hex color for UI)
            total_tv_조  : float (합산 거래대금, 조 단위)
            kospi_tv_조  : float
            kosdaq_tv_조 : float
            up_ratio     : float (상승 종목 비율 %)
            avg_change   : float (평균 등락률 %)
            volatility   : float (등락률 표준편차)
            limit_up_cnt : int (상한가 종목 수)
            limit_dn_cnt : int (하한가 종목 수)
            big_mover_cnt: int (거래대금·변동성 상위 대형 세력주)
            score        : float (종합 점수 0-100)
    """
    if daily_df.empty:
        return _empty_regime()

    has_market = "시장" in daily_df.columns

    # ── 거래대금 ──
    kospi_tv = 0.0
    kosdaq_tv = 0.0
    if has_market and "거래대금" in daily_df.columns:
        kospi_tv = daily_df.loc[daily_df["시장"] == "KOSPI", "거래대금"].sum() / 1e12
        kosdaq_tv = daily_df.loc[daily_df["시장"] == "KOSDAQ", "거래대금"].sum() / 1e12
    elif "거래대금" in daily_df.columns:
        kospi_tv = daily_df["거래대금"].sum() / 1e12
    total_tv = kospi_tv + kosdaq_tv

    # ── 등락 분포 ──
    changes = daily_df["등락률"] if "등락률" in daily_df.columns else pd.Series(dtype=float)
    up_ratio = (changes > 0).mean() * 100 if len(changes) > 0 else 50.0
    avg_change = changes.mean() if len(changes) > 0 else 0.0
    volatility = changes.std() if len(changes) > 0 else 0.0

    # ── 상한가/하한가 ──
    limit_up_cnt = int((changes >= 29.0).sum()) if len(changes) > 0 else 0
    limit_dn_cnt = int((changes <= -29.0).sum()) if len(changes) > 0 else 0

    # ── 대형 세력주 (거래대금 상위 + 변동성 상위) ──
    big_mover_cnt = 0
    if "거래대금" in daily_df.columns and len(changes) > 0:
        tv_top = daily_df.nlargest(50, "거래대금").index
        vol_mask = changes.abs() >= 10
        big_mover_cnt = int(daily_df.loc[daily_df.index.isin(tv_top) & vol_mask].shape[0])

    # ── 종합 점수 산출 (0~100) ──
    tv_score = min(total_tv / 15.0, 1.0) * 100       # 15조 기준
    breadth_score = min(up_ratio / 60, 1.0) * 100     # 60% 이상이면 만점
    vol_score = min(volatility / 3.0, 1.0) * 100      # 변동성 3% 기준
    limit_score = min(limit_up_cnt / 10.0, 1.0) * 100 # 상한가 10개 기준

    score = tv_score * 0.40 + breadth_score * 0.25 + vol_score * 0.20 + limit_score * 0.15
    score = max(0, min(100, score))

    # ── 레짐 분류 ──
    if score >= 65:
        level, label, color = 3, "과열(호황)", "#dc2626"
    elif score >= 35:
        level, label, color = 2, "보통", "#f59e0b"
    else:
        level, label, color = 1, "냉각장", "#6b7280"

    return {
        "level": level,
        "label": label,
        "color": color,
        "total_tv_조": round(total_tv, 2),
        "kospi_tv_조": round(kospi_tv, 2),
        "kosdaq_tv_조": round(kosdaq_tv, 2),
        "up_ratio": round(up_ratio, 1),
        "avg_change": round(avg_change, 2),
        "volatility": round(volatility, 2),
        "limit_up_cnt": limit_up_cnt,
        "limit_dn_cnt": limit_dn_cnt,
        "big_mover_cnt": big_mover_cnt,
        "score": round(score, 1),
        "tv_score": round(tv_score, 1),
        "breadth_score": round(breadth_score, 1),
        "vol_score": round(vol_score, 1),
        "limit_score": round(limit_score, 1),
    }


def _empty_regime() -> Dict:
    return {
        "level": 1, "label": "데이터없음", "color": "#94a3b8",
        "total_tv_조": 0, "kospi_tv_조": 0, "kosdaq_tv_조": 0,
        "up_ratio": 0, "avg_change": 0, "volatility": 0,
        "limit_up_cnt": 0, "limit_dn_cnt": 0, "big_mover_cnt": 0,
        "score": 0, "tv_score": 0, "breadth_score": 0, "vol_score": 0, "limit_score": 0,
    }


# ═══════════════════════════════════════════════════════════════════════════
# 포지션 규모 산출
# ═══════════════════════════════════════════════════════════════════════════

def suggest_position_size(regime: Dict, pnl_history: List[float] = None) -> Dict:
    """시장 레짐 + PnL 이력 기반 권장 포지션 규모.

    Args:
        regime: calc_market_regime() 반환 dict
        pnl_history: 최근 N일 일별 손익률(%) 리스트 (예: [1.2, -0.5, 0.8, ...])

    Returns:
        dict:
            base_pct       : int (레짐 기반 기본 비중 %)
            pnl_adj_pct    : int (PnL 조절 후 비중 %)
            final_pct      : int (최종 권장 비중 %)
            grade          : str ("풀배팅" / "공격적" / "중립" / "보수적" / "관망")
            grade_color    : str
            reason         : str (판단 근거)
            pnl_warning    : str | None (손실 경고 메시지)
    """
    level = regime.get("level", 2)
    score = regime.get("score", 50)

    # ── 레짐 기반 베이스 비중 ──
    if level == 3:
        base_pct = min(int(score * 0.9), 90)
    elif level == 2:
        base_pct = min(int(score * 0.7), 70)
    else:
        base_pct = min(int(score * 0.5), 40)
    base_pct = max(10, base_pct)

    # ── PnL 기반 조절 ──
    pnl_adj = 0
    pnl_warning = None
    if pnl_history and len(pnl_history) > 0:
        pnl_adj, pnl_warning = calc_pnl_based_sizing(pnl_history)

    final_pct = max(5, min(100, base_pct + pnl_adj))

    # ── 등급 ──
    if final_pct >= 80:
        grade, grade_color = "풀배팅", "#dc2626"
    elif final_pct >= 60:
        grade, grade_color = "공격적", "#ea580c"
    elif final_pct >= 40:
        grade, grade_color = "중립", "#f59e0b"
    elif final_pct >= 20:
        grade, grade_color = "보수적", "#16a34a"
    else:
        grade, grade_color = "관망", "#6b7280"

    reason = (
        f"시장 레짐 '{regime['label']}'(점수 {score:.0f}) → 기본 {base_pct}%"
        f"{f', PnL 조절 {pnl_adj:+d}%' if pnl_adj != 0 else ''}"
        f" → 최종 {final_pct}%"
    )

    return {
        "base_pct": base_pct,
        "pnl_adj_pct": pnl_adj,
        "final_pct": final_pct,
        "grade": grade,
        "grade_color": grade_color,
        "reason": reason,
        "pnl_warning": pnl_warning,
    }


def calc_pnl_based_sizing(pnl_list: List[float]) -> Tuple[int, str]:
    """수익/손실 이력 기반 비중 조절량과 경고 메시지 반환.

    리버패스 스타일: "수익을 담보로 비중을 조금씩 늘리고, 손실 나면 다시 줄인다."

    Args:
        pnl_list: 최근 N일 손익률(%) 리스트 (최신순)

    Returns:
        (adj_pct: int, warning: str|None)
        adj_pct > 0 이면 비중 상향, < 0 이면 축소
    """
    if not pnl_list:
        return 0, None

    arr = np.array(pnl_list, dtype=float)
    recent_3 = arr[:3] if len(arr) >= 3 else arr
    total_pnl = float(recent_3.sum())
    streak = _calc_streak(arr)

    adj = 0
    warning = None

    # ── 연속 수익 → 비중 상향 ──
    if streak >= 3:
        adj = min(streak * 5, 20)  # 연속 3일 +15%, 최대 +20%
    elif total_pnl >= 3.0:
        adj = 10
    elif total_pnl >= 1.0:
        adj = 5

    # ── 연속 손실 / 큰 손실 → 비중 축소 + 경고 ──
    if streak <= -3:
        adj = max(streak * 5, -30)
        warning = f"⚠️ 연속 {abs(streak)}일 손실 — 비중 {adj}% 축소 권장. 포지션을 줄이세요."
    elif total_pnl <= -5.0:
        adj = -20
        warning = f"🔴 최근 3일 누적 손실 {total_pnl:.1f}% — 비중 대폭 축소 권장."
    elif total_pnl <= -2.0:
        adj = -10
        warning = f"🟡 최근 3일 누적 손실 {total_pnl:.1f}% — 비중 소폭 축소 권장."

    return adj, warning


def _calc_streak(arr: np.ndarray) -> int:
    """연속 수익/손실 일수. 양수=연속 수익, 음수=연속 손실."""
    if len(arr) == 0:
        return 0
    streak = 0
    direction = 1 if arr[0] > 0 else -1 if arr[0] < 0 else 0
    for val in arr:
        if direction > 0 and val > 0:
            streak += 1
        elif direction < 0 and val < 0:
            streak -= 1
        else:
            break
    return streak


# ═══════════════════════════════════════════════════════════════════════════
# 종합지수 기반 "쉬어가기" 신호
# ═══════════════════════════════════════════════════════════════════════════

def check_market_rest_signal(index_df: pd.DataFrame) -> Dict:
    """KOSPI/KOSDAQ 종합지수 데이터를 분석하여 '쉬어가기' 신호를 반환.

    판단 기준:
    1) 종가 < 20일 이동평균 (단기 하락 추세)
    2) 20일 이평 < 60일 이평 (데드크로스 구간)
    3) 최근 5일 연속 하락
    4) 최근 5거래일 누적 수익률 -3% 이하 (급락)

    Args:
        index_df: 지수 일봉 DataFrame (columns: 종가, 등락률 등)

    Returns:
        dict:
            should_rest   : bool  — True이면 쉬어가기 강력 권고
            caution       : bool  — True이면 주의
            level         : str   — "REST" / "CAUTION" / "OK"
            reasons       : list[str] — 판단 근거 목록
            score         : int   — 위험도 (0~100, 높을수록 위험)
            ma20          : float
            ma60          : float
            current_price : float
            weekly_return : float
    """
    result = {
        "should_rest": False, "caution": False, "level": "OK",
        "reasons": [], "score": 0,
        "ma20": 0, "ma60": 0, "current_price": 0, "weekly_return": 0,
    }

    if index_df.empty or "종가" not in index_df.columns or len(index_df) < 5:
        return result

    close = index_df["종가"].astype(float)
    current = float(close.iloc[-1])
    result["current_price"] = current

    # ── 이동평균 계산 ──
    ma20 = float(close.rolling(20).mean().iloc[-1]) if len(close) >= 20 else 0
    ma60 = float(close.rolling(60).mean().iloc[-1]) if len(close) >= 60 else 0
    result["ma20"] = round(ma20, 2)
    result["ma60"] = round(ma60, 2)

    danger_score = 0
    reasons = []

    # 기준 1: 종가 < 20MA (단기 하락)
    if ma20 > 0 and current < ma20:
        danger_score += 25
        pct_below = (ma20 - current) / ma20 * 100
        reasons.append(f"종가가 20일 이평 아래 (-{pct_below:.1f}%)")

    # 기준 2: 20MA < 60MA (데드크로스 구간)
    if ma20 > 0 and ma60 > 0 and ma20 < ma60:
        danger_score += 30
        reasons.append("20일선이 60일선 아래 (데드크로스 구간)")

    # 기준 3: 5일 연속 하락
    recent_5 = close.tail(5)
    if len(recent_5) == 5:
        consecutive_down = all(
            recent_5.iloc[i] < recent_5.iloc[i - 1] for i in range(1, 5)
        )
        if consecutive_down:
            danger_score += 25
            reasons.append("5거래일 연속 하락")

    # 기준 4: 5일 누적 수익률 -3% 이하
    if len(close) >= 6:
        weekly_ret = (current / float(close.iloc[-6]) - 1) * 100
        result["weekly_return"] = round(weekly_ret, 2)
        if weekly_ret <= -3.0:
            danger_score += 20
            reasons.append(f"5거래일 수익률 {weekly_ret:.1f}% (급락)")
        elif weekly_ret <= -1.5:
            danger_score += 10
            reasons.append(f"5거래일 수익률 {weekly_ret:.1f}% (약세)")

    result["score"] = min(danger_score, 100)
    result["reasons"] = reasons

    if danger_score >= 50:
        result["should_rest"] = True
        result["level"] = "REST"
    elif danger_score >= 25:
        result["caution"] = True
        result["level"] = "CAUTION"

    return result


# ═══════════════════════════════════════════════════════════════════════════
# 20일 평균 거래대금 비교
# ═══════════════════════════════════════════════════════════════════════════

def calc_20day_avg_ratio(
    current_tv_조: float, historical_tvs: List[float] = None
) -> Dict:
    """현재 거래대금 대비 20일 평균 비율.

    Args:
        current_tv_조: 오늘 합산 거래대금 (조 단위)
        historical_tvs: 최근 20거래일 일별 합산 거래대금 리스트 (조 단위)

    Returns:
        dict: avg_20d, ratio, label
    """
    if not historical_tvs or len(historical_tvs) == 0:
        return {"avg_20d": 0, "ratio": 0, "label": "N/A"}

    avg_20d = float(np.mean(historical_tvs))
    ratio = current_tv_조 / avg_20d if avg_20d > 0 else 0

    if ratio >= 1.5:
        label = "📈 활발 (20일 평균 대비 {:.0f}%)".format(ratio * 100)
    elif ratio >= 0.8:
        label = "📊 보통 (20일 평균 대비 {:.0f}%)".format(ratio * 100)
    else:
        label = "📉 위축 (20일 평균 대비 {:.0f}%)".format(ratio * 100)

    return {"avg_20d": round(avg_20d, 2), "ratio": round(ratio, 2), "label": label}


# ═══════════════════════════════════════════════════════════════════════════
# 상한가 특수 로직
# ═══════════════════════════════════════════════════════════════════════════

def calc_limit_up_signals(daily_df: pd.DataFrame) -> pd.DataFrame:
    """상한가 근처 종목에 대한 특수 시그널 컬럼 추가.

    추가 컬럼:
        is_limit_up     : bool (상한가 도달 여부)
        is_near_limit   : bool (25%+ 근접)
        limit_zone      : str ("상한가" / "근접" / "일반")
    """
    if daily_df.empty or "등락률" not in daily_df.columns:
        return daily_df

    df = daily_df.copy()
    df["is_limit_up"] = df["등락률"] >= 29.0
    df["is_near_limit"] = (df["등락률"] >= 25.0) & (df["등락률"] < 29.0)
    df["limit_zone"] = "일반"
    df.loc[df["is_near_limit"], "limit_zone"] = "근접"
    df.loc[df["is_limit_up"], "limit_zone"] = "상한가"
    return df
