"""
멀티팩터 스코어링 모듈 (Multi-factor Scoring)

종합 점수 0~100 계산 구성:
- Institutional Accumulation (40%): 최근 3일 기관/외인 동반 순매수 강도
- Price Momentum        (30%): VWAP 돌파 여부 + 5일 이동평균선 이격도
- Volume Surge         (30%): 최근 20일 평균 대비 당일 거래량 급증 비율

Anomaly Detection:
- 최근 10거래일 거래량이 소외(저조)하다가 갑자기 수급 유입 → 소외주 반등 탐지
"""

import numpy as np
import pandas as pd
from typing import Tuple


# ---------------------------------------------------------------------------
# 개별 팩터 점수
# ---------------------------------------------------------------------------


def calc_institutional_score(
    investor_df: pd.DataFrame,
    ohlcv: pd.DataFrame,
) -> float:
    """
    기관/외인 동반 순매수 강도 점수 (0~100, 가중치 40%).

    - 최근 3거래일 기관+외인 쌍끌이 일수 비율 (60%)
    - 순매수 강도: 거래대금(거래량×종가) 대비 순매수 비율 (40%)

    investor_df: index=날짜, columns=[기관합계, 외국인합계, 개인] (원)
    ohlcv: index=날짜, columns=[종가, 거래량, ...]
    """
    if investor_df.empty:
        return 0.0

    recent_inv = investor_df.tail(3)

    # 1) 동반 순매수 일수 비율 (기관 > 0 AND 외인 > 0)
    both_positive = (
        (recent_inv["기관합계"] > 0) & (recent_inv["외국인합계"] > 0)
    ).sum()
    day_ratio = both_positive / max(1, len(recent_inv))

    # 2) 순매수 강도 vs 거래대금 (거래량×종가 근사)
    total_net = (recent_inv["기관합계"] + recent_inv["외국인합계"]).sum()

    if not ohlcv.empty and "거래량" in ohlcv.columns and "종가" in ohlcv.columns:
        recent_tv = (ohlcv["거래량"] * ohlcv["종가"]).tail(3).sum()
    else:
        recent_tv = 0

    if recent_tv > 0 and total_net > 0:
        # 순매수비율 → log scale 정규화 (5% ≈ 50점, 20% ≈ 100점)
        intensity_ratio = min(1.0, total_net / recent_tv)
        intensity_score = min(
            1.0, np.log1p(intensity_ratio * 20) / np.log1p(20)
        )
    elif total_net > 0:
        intensity_score = 0.3
    else:
        intensity_score = 0.0

    score = (day_ratio * 0.6 + intensity_score * 0.4) * 100
    return round(min(100.0, max(0.0, score)), 1)


def calc_momentum_score(ohlcv: pd.DataFrame) -> float:
    """
    가격 모멘텀 점수 (0~100, 가중치 30%).

    - 거래량 가중 평균가(VWAP) 돌파 여부 (50%)
    - 5일 이동평균선 이격도 (50%)

    VWAP 근사: Σ(종가×거래량) / Σ(거래량)  over 최근 20거래일
    이격도: (현재가 - MA5) / MA5 × 100
    """
    if ohlcv.empty or len(ohlcv) < 5:
        return 0.0

    close = ohlcv["종가"]
    volume = ohlcv["거래량"]
    last_close = close.iloc[-1]

    # 1) Rolling VWAP (최대 20일)
    window = min(20, len(ohlcv))
    pv_sum = (close * volume).tail(window).sum()
    vol_sum = volume.tail(window).sum()
    vwap = pv_sum / vol_sum if vol_sum > 0 else last_close

    vwap_score = 1.0 if last_close > vwap else 0.2

    # 2) 5일 MA 이격도
    ma5 = close.tail(5).mean()
    if ma5 > 0:
        deviation = (last_close - ma5) / ma5 * 100
        # 이격도 점수: −5% → 0점, 0% → 50점, +5% → 100점
        dev_score = min(1.0, max(0.0, (deviation + 5) / 10))
    else:
        dev_score = 0.5

    score = (vwap_score * 0.5 + dev_score * 0.5) * 100
    return round(min(100.0, max(0.0, score)), 1)


def calc_volume_surge_score(ohlcv: pd.DataFrame) -> float:
    """
    거래량 급증 점수 (0~100, 가중치 30%).

    직전 20일 평균 대비 당일 거래량 비율:
    - 1배 → 0점, 2배 → 50점, 5배 이상 → 100점 (log scale)
    """
    if ohlcv.empty or len(ohlcv) < 21:
        return 0.0

    volume = ohlcv["거래량"]
    avg_vol_20 = volume.iloc[-21:-1].mean()
    today_vol = volume.iloc[-1]

    if avg_vol_20 == 0:
        return 0.0

    ratio = today_vol / avg_vol_20
    if ratio <= 1.0:
        return 0.0

    score = min(100.0, (np.log1p(ratio - 1) / np.log1p(4)) * 100)
    return round(score, 1)


# ---------------------------------------------------------------------------
# 소외주 반등 감지 (Anomaly Detection)
# ---------------------------------------------------------------------------


def is_anomaly_neglected_rebound(ohlcv: pd.DataFrame) -> bool:
    """
    소외주 반등 감지.

    조건:
    - 최근 20거래일 중 앞 10거래일 평균 거래량 < 20일 평균의 30% (방치 구간)
    - 최근 3거래일 평균 거래량 ≥ 20일 평균의 150% (수급 유입)
    """
    if ohlcv.empty or len(ohlcv) < 20:
        return False

    volume = ohlcv["거래량"].tail(20)
    avg_20 = volume.mean()
    if avg_20 == 0:
        return False

    # 앞 10일(방치 구간)은 시계열 기준 오래된 쪽
    avg_first10 = volume.iloc[:10].mean()
    avg_last3 = volume.iloc[-3:].mean()

    neglected = avg_first10 < avg_20 * 0.30
    rebounding = avg_last3 > avg_20 * 1.50

    return bool(neglected and rebounding)


# ---------------------------------------------------------------------------
# 선행 매집 신호 감지 (Early Accumulation Signal)
# ---------------------------------------------------------------------------


def calc_early_accumulation_score(
    ohlcv: pd.DataFrame,
    investor_df: pd.DataFrame,
) -> Tuple[float, bool]:
    """
    선행 매집 + 초기 이동 신호 점수 (0~100).

    목적: 급등 전 초기 움직임(3~15%)을 가진 종목을 조기 포착.

    3일 수익률에 따른 처리:
    - 0 ~ 3%   : 아직 본격 이동 없음 (중립)
    - 3% ~ 15% : ✅ 초기 이동 신호 → 보너스 +30점 (이게 핵심 타이밍)
    - 15% ~ 25%: 중간 단계 → 감점 -20점 (조금 늦음)
    - 25% 초과  : ❌ 이미 급등 → 종합점수 0 (너무 늦음)

    구성:
    - 초기 이동 보너스 (3~15% 상승 구간)    : +30점 가산
    - 가격 박스권 + 거래량 증가 (매집 패턴) : 최대 40점
    - 기관의 꾸준한 조용한 순매수           : 최대 30점

    반환: (score, is_already_surged)
    """
    if ohlcv.empty or len(ohlcv) < 20:
        return 0.0, False

    close = ohlcv["종가"]
    volume = ohlcv["거래량"]

    # ── 수익률 계산 ──
    ret_3d = (close.iloc[-1] / close.iloc[-4] - 1) * 100 if len(close) >= 4 else 0.0
    ret_5d = (close.iloc[-1] / close.iloc[-6] - 1) * 100 if len(close) >= 6 else 0.0

    # 이미 너무 많이 오른 종목 → 추천 제외
    is_already_surged = ret_3d > 25.0 or ret_5d > 35.0
    if is_already_surged:
        return 0.0, True

    score = 0.0

    # ── 1) 초기 이동 보너스 ──
    # 3~15% 오른 구간 = 막 움직이기 시작한 골든타임
    if 3.0 <= ret_3d <= 15.0:
        # 7% 전후 최대 점수, 양끝으로 갈수록 감소 (포물선)
        bonus = 30 - abs(ret_3d - 7.0) * 1.5
        score += max(0, bonus)
    elif 15.0 < ret_3d <= 25.0:
        # 조금 늦음 → 소폭 감점
        score -= 20

    # ── 2) 가격 박스권 + 거래량 증가 패턴 ──
    price_10d = close.tail(10)
    price_range_pct = (price_10d.max() - price_10d.min()) / price_10d.mean() * 100

    avg_vol_20 = volume.iloc[-21:-1].mean() if len(volume) >= 21 else volume.mean()
    avg_vol_10 = volume.tail(10).mean()

    if avg_vol_20 > 0:
        vol_ratio = avg_vol_10 / avg_vol_20
        if price_range_pct < 10 and vol_ratio > 1.3:
            # 가격 박스 + 거래량 증가 → 강한 매집
            score += 40
        elif price_range_pct < 15 and vol_ratio > 1.1:
            score += 20

    # ── 3) 기관의 꾸준한 조용한 순매수 ──
    if not investor_df.empty and len(investor_df) >= 10:
        recent_10d = investor_df.tail(10)
        if "기관합계" in recent_10d.columns:
            inst_positive_days = int((recent_10d["기관합계"] > 0).sum())
            if inst_positive_days >= 7:
                score += 30
            elif inst_positive_days >= 5:
                score += 15
            elif inst_positive_days >= 3:
                score += 5
            elif inst_positive_days >= 3:
                score += 10

    return round(min(100.0, max(0.0, score)), 1), False


# ---------------------------------------------------------------------------
# 종합 점수 계산
# ---------------------------------------------------------------------------


def calc_composite_score(
    ohlcv: pd.DataFrame,
    investor_df: pd.DataFrame,
) -> Tuple[float, dict]:
    """
    멀티팩터 종합 점수 계산 (0~100).

    가중치 (v2 - 선행 신호 강화):
    - 수급강도       30%  (기존 40% → 단기 3일 집중매수는 이미 늦을 수 있음)
    - 가격모멘텀     20%  (기존 30% → 이미 오른 모멘텀 비중 축소)
    - 거래량급증     20%  (기존 30% → 당일 폭발 거래량은 후행)
    - 선행매집신호   30%  (신규 → 급등 이전 조용한 매집 포착)

    급등 후 페널티:
    - 3일 수익률 15% 초과 or 5일 수익률 25% 초과 → 총점 0 (추천 제외)

    반환:
        total_score (float): 종합 점수
        details (dict): 세부 점수 + 소외주 반등 여부 + 급등 여부
    """
    inst = calc_institutional_score(investor_df, ohlcv)
    momentum = calc_momentum_score(ohlcv)
    volume = calc_volume_surge_score(ohlcv)
    early, is_surged = calc_early_accumulation_score(ohlcv, investor_df)
    anomaly = is_anomaly_neglected_rebound(ohlcv)

    # 이미 급등한 종목은 추천에서 제외
    if is_surged:
        total = 0.0
    else:
        total = round(
            inst * 0.30
            + momentum * 0.20
            + volume * 0.20
            + early * 0.30,
            1,
        )

    details = {
        "수급강도점수": inst,
        "가격모멘텀점수": momentum,
        "거래량급증점수": volume,
        "선행매집점수": early,
        "소외주반등": anomaly,
        "이미급등": is_surged,
        "종합점수": total,
    }
    return total, details
