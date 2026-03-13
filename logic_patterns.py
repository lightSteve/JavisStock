"""
매매 유형별 패턴 감지 로직 (A-E)

Type A : 테마 1등주 추격 (상따·준상따, 짝꿍)
Type B : 뉴스/속보 스파이크
Type C : 전고/신고가 돌파
Type D : 바이오 루머 급락 → 회복
Type E : 단기 스윙 포지션 관리 헬퍼
"""

from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np


# ═══════════════════════════════════════════════════════════════════════════
# Type A : 테마 1등주 추격
# ═══════════════════════════════════════════════════════════════════════════

def detect_theme_leaders(daily_df: pd.DataFrame, theme_df: pd.DataFrame = None) -> pd.DataFrame:
    """테마별 1등주(대장주) + 2등주(후발) 후보 감지.

    daily_df에 등락률·거래대금 필수. theme_df는 get_theme_list() 결과.
    Returns: DataFrame(테마명, rank, ticker, 종목명, 등락률, 거래대금, signal)
    """
    results = []
    if daily_df.empty:
        return pd.DataFrame(results)

    # 업종 기반 그룹핑 (테마 데이터 없을 때 대체)
    group_col = "업종" if "업종" in daily_df.columns else None
    if group_col is None:
        return pd.DataFrame(results)

    for sector, grp in daily_df.groupby(group_col):
        if len(grp) < 2:
            continue
        top2 = grp.nlargest(2, "등락률")
        for rank_i, (ticker, row) in enumerate(top2.iterrows()):
            results.append({
                "테마명": sector,
                "rank": rank_i + 1,
                "ticker": ticker,
                "종목명": row.get("종목명", ticker),
                "등락률": row.get("등락률", 0),
                "거래대금": row.get("거래대금", 0),
                "signal": "대장주" if rank_i == 0 else "후발주",
            })

    return pd.DataFrame(results)


def detect_limit_up_pairs(daily_df: pd.DataFrame) -> pd.DataFrame:
    """상한가 종목과 같은 업종 내 후발 주자 매칭.

    Returns: DataFrame(대장_ticker, 대장_종목명, 대장_등락률,
                       후발_ticker, 후발_종목명, 후발_등락률, 업종)
    """
    pairs = []
    if daily_df.empty or "등락률" not in daily_df.columns:
        return pd.DataFrame(pairs)

    limit_ups = daily_df[daily_df["등락률"] >= 29.0]
    if limit_ups.empty or "업종" not in daily_df.columns:
        return pd.DataFrame(pairs)

    for ticker, row in limit_ups.iterrows():
        sector = row.get("업종", "")
        if not sector:
            continue
        same_sector = daily_df[
            (daily_df["업종"] == sector) &
            (daily_df.index != ticker) &
            (daily_df["등락률"] > 0)
        ].nlargest(3, "등락률")

        for t2, r2 in same_sector.iterrows():
            pairs.append({
                "대장_ticker": ticker,
                "대장_종목명": row.get("종목명", ticker),
                "대장_등락률": row["등락률"],
                "후발_ticker": t2,
                "후발_종목명": r2.get("종목명", t2),
                "후발_등락률": r2["등락률"],
                "업종": sector,
            })

    return pd.DataFrame(pairs)


# ═══════════════════════════════════════════════════════════════════════════
# Type B : 뉴스 스파이크
# ═══════════════════════════════════════════════════════════════════════════

_NEWS_IMPACT_KEYWORDS = {
    "high": ["단독", "FDA", "승인", "합병", "인수", "수주", "계약", "허가", "정부", "대통령", "긴급"],
    "medium": ["특징주", "급등", "상한가", "테마", "MOU", "공급", "정책", "국회"],
    "low": ["실적", "배당", "전망", "분석", "리포트", "목표가"],
}


def calc_news_impact_score(news_list: List[Dict]) -> float:
    """뉴스 리스트에 대한 임팩트 스코어 산출 (0~100).

    news_list: [{title: str, ...}, ...] 형태
    """
    if not news_list:
        return 0.0

    score = 0.0
    for item in news_list:
        title = str(item.get("title", ""))
        for kw in _NEWS_IMPACT_KEYWORDS["high"]:
            if kw in title:
                score += 15
        for kw in _NEWS_IMPACT_KEYWORDS["medium"]:
            if kw in title:
                score += 8
        for kw in _NEWS_IMPACT_KEYWORDS["low"]:
            if kw in title:
                score += 3

    return min(100.0, score)


def detect_news_spike_candidates(daily_df: pd.DataFrame) -> pd.DataFrame:
    """거래량 급증 + 등락률 상위 종목 중 뉴스 스파이크 후보.

    Returns: daily_df subset with added 'spike_score' column.
    """
    if daily_df.empty or "등락률" not in daily_df.columns:
        return pd.DataFrame()

    df = daily_df.copy()

    # 거래대금 상위 + 양봉 필터
    if "거래대금" in df.columns:
        min_tv = df["거래대금"].quantile(0.7)
        candidates = df[(df["등락률"] > 3) & (df["거래대금"] >= min_tv)]
    else:
        candidates = df[df["등락률"] > 3]

    if candidates.empty:
        return pd.DataFrame()

    # 스파이크 점수 (등락률 + 거래대금 비중)
    candidates = candidates.copy()
    max_change = candidates["등락률"].max()
    candidates["spike_score"] = (candidates["등락률"] / max(max_change, 1)) * 50
    if "거래대금" in candidates.columns:
        max_tv = candidates["거래대금"].max()
        candidates["spike_score"] += (candidates["거래대금"] / max(max_tv, 1)) * 50

    return candidates.sort_values("spike_score", ascending=False)


# ═══════════════════════════════════════════════════════════════════════════
# Type C : 전고/신고가 돌파
# ═══════════════════════════════════════════════════════════════════════════

def detect_breakout_candidates(daily_df: pd.DataFrame, ohlcv_history: pd.DataFrame = None) -> pd.DataFrame:
    """당일 전고점(20일 고가) 돌파 후보 감지.

    daily_df에 종가·고가가 있는 경우 간이 판별.
    ohlcv_history가 있으면 정밀 판별.
    """
    if daily_df.empty:
        return pd.DataFrame()

    df = daily_df.copy()
    # 간이: 당일 양봉 + 상위 거래대금
    if "등락률" in df.columns and "거래대금" in df.columns:
        breakout = df[(df["등락률"] > 2) & (df["거래대금"] > df["거래대금"].median())]
        breakout = breakout.copy()
        breakout["breakout_type"] = "간이_전고돌파후보"
        return breakout.sort_values("등락률", ascending=False)

    return pd.DataFrame()


def check_52week_high(ticker: str, ohlcv: pd.DataFrame) -> Dict:
    """52주 신고가 돌파 여부 확인.

    Args:
        ticker: 종목코드
        ohlcv: 최소 250거래일 OHLCV (cols: 종가, 고가)

    Returns:
        dict: is_new_high, high_52w, current_price, gap_pct
    """
    if ohlcv.empty or len(ohlcv) < 5:
        return {"is_new_high": False, "high_52w": 0, "current_price": 0, "gap_pct": 0}

    high_col = "고가" if "고가" in ohlcv.columns else "종가"
    high_52w = ohlcv[high_col].iloc[:-1].max() if len(ohlcv) > 1 else ohlcv[high_col].iloc[0]
    current = ohlcv["종가"].iloc[-1]
    gap_pct = ((current - high_52w) / high_52w * 100) if high_52w > 0 else 0

    return {
        "is_new_high": current >= high_52w,
        "high_52w": high_52w,
        "current_price": current,
        "gap_pct": round(gap_pct, 2),
    }


def check_swing_breakout(ohlcv: pd.DataFrame, lookback: int = 20) -> Dict:
    """최근 N일 박스권 돌파 여부.

    Args:
        ohlcv: OHLCV DataFrame (cols: 종가, 고가)
        lookback: 박스권 산출 기간

    Returns:
        dict: is_breakout, box_high, box_low, current, direction
    """
    if ohlcv.empty or len(ohlcv) < lookback + 1:
        return {"is_breakout": False, "box_high": 0, "box_low": 0, "current": 0, "direction": ""}

    high_col = "고가" if "고가" in ohlcv.columns else "종가"
    low_col = "저가" if "저가" in ohlcv.columns else "종가"

    box = ohlcv.iloc[-(lookback + 1):-1]
    box_high = box[high_col].max()
    box_low = box[low_col].min()
    current = ohlcv["종가"].iloc[-1]

    is_breakout = current > box_high or current < box_low
    direction = "상방돌파" if current > box_high else "하방이탈" if current < box_low else ""

    return {
        "is_breakout": is_breakout,
        "box_high": box_high,
        "box_low": box_low,
        "current": current,
        "direction": direction,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Type D : 바이오 급락 → 회복
# ═══════════════════════════════════════════════════════════════════════════

_BIO_KEYWORDS = [
    "바이오", "제약", "의약", "헬스", "셀", "진단", "신약",
    "임상", "의료", "생명", "줄기세포", "항체", "백신", "유전자",
]


def is_bio_stock(row: pd.Series) -> bool:
    """바이오/제약 종목 여부."""
    text = str(row.get("업종", "")) + " " + str(row.get("종목명", ""))
    return any(kw in text for kw in _BIO_KEYWORDS)


def detect_bio_crash(daily_df: pd.DataFrame, threshold: float = -5.0) -> pd.DataFrame:
    """바이오 종목 급락 감지.

    Returns: 바이오 종목 중 threshold 이하 등락률 종목
    """
    if daily_df.empty or "등락률" not in daily_df.columns:
        return pd.DataFrame()

    bio_mask = daily_df.apply(is_bio_stock, axis=1)
    bio = daily_df[bio_mask]
    crash = bio[bio["등락률"] <= threshold].sort_values("등락률")
    return crash


def calc_recovery_stats(ohlcv: pd.DataFrame, crash_idx: int = -1) -> Dict:
    """급락 후 회복률 통계.

    Args:
        ohlcv: OHLCV 히스토리
        crash_idx: 급락 발생 인덱스 (기본 마지막)

    Returns:
        dict: crash_pct, days_since, recovery_pct, is_recovering
    """
    if ohlcv.empty or len(ohlcv) < 3:
        return {"crash_pct": 0, "days_since": 0, "recovery_pct": 0, "is_recovering": False}

    prices = ohlcv["종가"].values
    crash_price = prices[crash_idx]
    current_price = prices[-1]

    # 급락 전 고점
    if crash_idx < 0:
        crash_idx = len(prices) + crash_idx
    pre_high = prices[:max(crash_idx, 1)].max()

    crash_pct = ((crash_price - pre_high) / pre_high * 100) if pre_high > 0 else 0
    recovery_pct = ((current_price - crash_price) / crash_price * 100) if crash_price > 0 else 0
    days_since = len(prices) - 1 - crash_idx

    return {
        "crash_pct": round(crash_pct, 2),
        "days_since": days_since,
        "recovery_pct": round(recovery_pct, 2),
        "is_recovering": recovery_pct > 0,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Type D (확장): 전체 섹터 흐름 + 급락→회복 패턴
# ═══════════════════════════════════════════════════════════════════════════

_SECTOR_PHASE_THRESHOLDS = {
    "급락": -5.0,   # 평균 등락률 -5% 이하
    "하락": -1.0,   # -5% ~ -1%
    "회복": 2.0,    # -1% ~ +2%  (약한 양봉, 하락 후 반등)
    "상승": 100.0,  # +2% 이상
}


def classify_sector_phase(avg_change: float) -> str:
    """섹터 평균 등락률로 국면 분류."""
    if avg_change <= _SECTOR_PHASE_THRESHOLDS["급락"]:
        return "급락"
    elif avg_change <= _SECTOR_PHASE_THRESHOLDS["하락"]:
        return "하락"
    elif avg_change <= _SECTOR_PHASE_THRESHOLDS["회복"]:
        return "회복"
    else:
        return "상승"


def analyze_all_sectors(daily_df: pd.DataFrame) -> pd.DataFrame:
    """전체 섹터의 현재 흐름을 분석하여 요약 DataFrame을 반환.

    Returns: DataFrame with columns:
        섹터, 종목수, 평균등락률, 상승비율, 합산거래대금_억,
        급락종목수, 회복종목수, 국면, 국면색상
    """
    if daily_df.empty or "업종" not in daily_df.columns:
        return pd.DataFrame()

    phase_meta = {
        "급락": {"icon": "🔴", "color": "#dc2626", "order": 0},
        "하락": {"icon": "🟠", "color": "#ea580c", "order": 1},
        "회복": {"icon": "🟡", "color": "#f59e0b", "order": 2},
        "상승": {"icon": "🟢", "color": "#16a34a", "order": 3},
    }

    rows = []
    for sector, grp in daily_df.groupby("업종"):
        if not sector or pd.isna(sector) or len(grp) < 2:
            continue

        changes = grp["등락률"] if "등락률" in grp.columns else pd.Series(dtype=float)
        avg_chg = changes.mean() if len(changes) > 0 else 0.0
        up_ratio = (changes > 0).mean() * 100 if len(changes) > 0 else 0.0
        total_tv = grp["거래대금"].sum() / 1e8 if "거래대금" in grp.columns else 0.0
        crash_cnt = int((changes <= -5.0).sum())
        recover_cnt = int(((changes > 0) & (changes <= 5.0)).sum()) if crash_cnt > 0 or avg_chg < 0 else 0

        phase = classify_sector_phase(avg_chg)
        meta = phase_meta[phase]

        rows.append({
            "섹터": sector,
            "종목수": len(grp),
            "평균등락률": round(avg_chg, 2),
            "상승비율": round(up_ratio, 1),
            "합산거래대금_억": round(total_tv, 0),
            "급락종목수": crash_cnt,
            "회복종목수": recover_cnt,
            "국면": phase,
            "국면아이콘": meta["icon"],
            "국면색상": meta["color"],
            "_order": meta["order"],
        })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows).sort_values("_order").drop(columns="_order").reset_index(drop=True)
    return result


def detect_sector_crash_stocks(
    daily_df: pd.DataFrame, sector: str, threshold: float = -5.0
) -> pd.DataFrame:
    """특정 섹터 내 급락 종목 추출."""
    if daily_df.empty or "업종" not in daily_df.columns:
        return pd.DataFrame()
    mask = (daily_df["업종"] == sector) & (daily_df["등락률"] <= threshold)
    return daily_df[mask].sort_values("등락률").copy()


def detect_sector_recovering_stocks(
    daily_df: pd.DataFrame, sector: str
) -> pd.DataFrame:
    """특정 섹터 내에서 급락 후 회복 신호를 보이는 종목.

    조건: 당일 양봉(등락률 > 0) + 거래대금 섹터 중앙값 이상
    (실제 급락→회복은 과거 데이터가 필요하지만, 당일 기준 간이 필터)
    """
    if daily_df.empty or "업종" not in daily_df.columns:
        return pd.DataFrame()

    sector_df = daily_df[daily_df["업종"] == sector].copy()
    if sector_df.empty:
        return pd.DataFrame()

    # 양봉 + 거래대금 활발
    median_tv = sector_df["거래대금"].median() if "거래대금" in sector_df.columns else 0
    mask = (sector_df["등락률"] > 0)
    if "거래대금" in sector_df.columns and median_tv > 0:
        mask = mask & (sector_df["거래대금"] >= median_tv)

    return sector_df[mask].sort_values("등락률", ascending=False).copy()


# ═══════════════════════════════════════════════════════════════════════════
# Type E : 단기 스윙 포지션 관리
# ═══════════════════════════════════════════════════════════════════════════

def calc_position_risk(
    entry_price: float,
    current_price: float,
    stop_loss_pct: float = -3.0,
    target_pct: float = 10.0,
    quantity: int = 1,
) -> Dict:
    """개별 포지션 리스크 계산.

    Args:
        entry_price: 매수가
        current_price: 현재가
        stop_loss_pct: 손절 기준(%)
        target_pct: 목표가 기준(%)
        quantity: 수량

    Returns:
        dict with pnl, pnl_pct, stop_price, target_price, risk_reward, status
    """
    if entry_price <= 0:
        return {"pnl": 0, "pnl_pct": 0, "stop_price": 0, "target_price": 0,
                "risk_reward": 0, "status": "invalid"}

    pnl_pct = (current_price - entry_price) / entry_price * 100
    pnl = (current_price - entry_price) * quantity

    stop_price = entry_price * (1 + stop_loss_pct / 100)
    target_price = entry_price * (1 + target_pct / 100)

    risk = abs(stop_loss_pct)
    reward = target_pct
    risk_reward = reward / risk if risk > 0 else 0

    if pnl_pct <= stop_loss_pct:
        status = "손절"
    elif pnl_pct >= target_pct:
        status = "목표도달"
    elif pnl_pct > 0:
        status = "수익중"
    else:
        status = "손실중"

    return {
        "pnl": round(pnl, 0),
        "pnl_pct": round(pnl_pct, 2),
        "stop_price": round(stop_price, 0),
        "target_price": round(target_price, 0),
        "risk_reward": round(risk_reward, 2),
        "status": status,
    }


def build_portfolio_summary(positions: List[Dict], daily_df: pd.DataFrame = None) -> Dict:
    """포트폴리오 전체 요약.

    Args:
        positions: [{ticker, entry_price, quantity, stop_loss_pct, target_pct, trade_type}, ...]
        daily_df: 현재가 조회용

    Returns:
        dict: total_value, total_pnl, total_pnl_pct, positions_detail, risk_summary
    """
    total_cost = 0.0
    total_value = 0.0
    details = []

    for pos in positions:
        ticker = pos.get("ticker", "")
        entry = pos.get("entry_price", 0)
        qty = pos.get("quantity", 0)
        sl = pos.get("stop_loss_pct", -3.0)
        tp = pos.get("target_pct", 10.0)
        trade_type = pos.get("trade_type", "")

        # 현재가 조회
        current = entry  # 기본값
        name = ticker
        if daily_df is not None and ticker in daily_df.index:
            current = daily_df.loc[ticker, "종가"] if "종가" in daily_df.columns else entry
            name = daily_df.loc[ticker, "종목명"] if "종목명" in daily_df.columns else ticker

        risk = calc_position_risk(entry, current, sl, tp, qty)

        total_cost += entry * qty
        total_value += current * qty

        details.append({
            "ticker": ticker,
            "종목명": name,
            "trade_type": trade_type,
            "entry_price": entry,
            "current_price": current,
            "quantity": qty,
            **risk,
        })

    total_pnl = total_value - total_cost
    total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0

    # 리스크 요약
    at_stop = sum(1 for d in details if d["status"] == "손절")
    at_target = sum(1 for d in details if d["status"] == "목표도달")
    winning = sum(1 for d in details if d["pnl_pct"] > 0)
    losing = sum(1 for d in details if d["pnl_pct"] < 0)

    return {
        "total_cost": round(total_cost, 0),
        "total_value": round(total_value, 0),
        "total_pnl": round(total_pnl, 0),
        "total_pnl_pct": round(total_pnl_pct, 2),
        "count": len(details),
        "winning": winning,
        "losing": losing,
        "at_stop": at_stop,
        "at_target": at_target,
        "positions": details,
    }
