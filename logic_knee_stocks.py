"""
"무릎 아래" 저평가 가치주 스크리닝 로직

기저/바닥 구간에서 반등 조짐이 보이는 종목을 찾아냅니다.
핵심 기준:
1. PBR 0.6~1.0 (자산 대비 저평가)
2. 기관/외인 조용한 매집 (급등 없이 꾸준 순매수)
3. 이평선 역배열→정배열 전환기 (골든크로스 임박/형성)
4. RSI 과매도 탈출 (30 이하 → 35~45 회복)
5. 수급 전환 (거래량 보통이나 순매수 비중 높음)
6. 볼린저 하단 반등 → 중심선 방향
7. ABCD 패턴 'C' 포인트 연계
"""

import time
import datetime
from typing import List, Dict, Optional

import pandas as pd
import numpy as np

from data.fetcher import get_stock_ohlcv_history, get_stock_fundamentals
from analysis.indicators import (
    calc_all_indicators,
    check_alignment,
    check_golden_cross,
)


# ═══════════════════════════════════════════════════════════════════════════
# 유틸
# ═══════════════════════════════════════════════════════════════════════════

def _safe_float(val, default=0.0) -> float:
    try:
        v = float(val)
        return default if np.isnan(v) else v
    except (ValueError, TypeError):
        return default


def _fetch_ohlcv(ticker: str, date: str, days: int = 120) -> Optional[pd.DataFrame]:
    try:
        end_dt = datetime.datetime.strptime(date, "%Y%m%d")
        start_dt = end_dt - datetime.timedelta(days=days)
        df = get_stock_ohlcv_history(ticker, start_dt.strftime("%Y%m%d"), date)
        if df.empty or len(df) < 20:
            return None
        return df
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════
# 1단계: daily_df 기반 빠른 사전 필터
# ═══════════════════════════════════════════════════════════════════════════

def _prefilter_candidates(daily_df: pd.DataFrame, max_candidates: int = 120) -> pd.DataFrame:
    """
    API 호출 없이 daily_df 컬럼만으로 후보를 추림.
    - 기관/외인 중 하나 이상 순매수 (또는 합산 양수)
    - 등락률 과격하지 않은 종목 (-8% ~ +8%)
    - 거래대금 최소 기준 이상
    """
    df = daily_df.copy()

    inst = df.get("기관합계_5일", pd.Series(0, index=df.index))
    frgn = df.get("외국인합계_5일", pd.Series(0, index=df.index))
    chg = df.get("등락률", pd.Series(0, index=df.index))
    vol_amt = df.get("거래대금", pd.Series(0, index=df.index))

    # 기관 OR 외인 순매수 > 0, 또는 합산이 양수
    supply_ok = (inst > 0) | (frgn > 0) | ((inst + frgn) > 0)
    # 등락률 온건
    change_ok = (chg >= -8) & (chg <= 8)
    # 거래대금 최소 (5억 미만 제외)
    vol_ok = vol_amt >= 500_000_000

    candidates = df[supply_ok & change_ok & vol_ok].copy()

    # 기관+외인 합산 순매수로 정렬, 상위 max_candidates
    candidates["_supply_sum"] = (
        candidates.get("기관합계_5일", 0) + candidates.get("외국인합계_5일", 0)
    )
    candidates = candidates.sort_values("_supply_sum", ascending=False).head(max_candidates)
    return candidates


# ═══════════════════════════════════════════════════════════════════════════
# 2단계: 기술적 지표 필터 + PBR 필터
# ═══════════════════════════════════════════════════════════════════════════

def _check_rsi_recovery(df: pd.DataFrame, lookback: int = 15) -> tuple:
    """
    최근 lookback 거래일 내 RSI 저점이 있었다가 현재 회복 여부.
    Returns: (is_recovery: bool, current_rsi: float, min_rsi: float)
    """
    if "RSI" not in df.columns or len(df) < lookback + 1:
        return False, 0.0, 0.0

    current_rsi = df["RSI"].iloc[-1]
    recent_rsi = df["RSI"].iloc[-(lookback + 1):-1]
    min_rsi = recent_rsi.min()

    # 완화: 최근 RSI가 40 이하였다가 현재 30~55 구간으로 회복
    is_recovery = (min_rsi <= 40) and (30 <= current_rsi <= 55) and (current_rsi > min_rsi + 3)
    return is_recovery, current_rsi, min_rsi


def _check_bollinger_bounce(df: pd.DataFrame) -> tuple:
    """
    볼린저 하단 터치 후 중심선 방향 반등.
    Returns: (is_bounce: bool, current_pctb: float)
    """
    if "BB_%B" not in df.columns or len(df) < 5:
        return False, 0.0

    current_pctb = df["BB_%B"].iloc[-1]
    recent_pctb = df["BB_%B"].iloc[-8:-1]  # 8거래일로 확대

    if pd.isna(current_pctb):
        return False, 0.0

    # 완화: 최근 8일 내 %B ≤ 0.25 터치 후 현재 0.0~0.6 (하단~중간 이동중)
    touched_lower = (recent_pctb <= 0.25).any()
    moving_up = 0.0 <= current_pctb <= 0.6

    return touched_lower and moving_up, float(current_pctb)


def _check_alignment_transition(df: pd.DataFrame) -> tuple:
    """
    역배열 → 정배열초기 전환기 or 골든크로스 형성 여부.
    Returns: (is_transition: bool, alignment: str, has_golden_cross: bool)
    """
    alignment = check_alignment(df)
    gc = check_golden_cross(df)

    # 정배열초기/완전정배열 이거나 골든크로스 발생
    # 역배열이어도 MA20이 MA60에 수렴 중이면 전환 임박으로 인정
    is_transition = (alignment in ("정배열초기", "완전정배열")) or gc

    if not is_transition and len(df) >= 60:
        last = df.iloc[-1]
        ma20 = last.get("MA20", 0)
        ma60 = last.get("MA60", 0)
        if ma60 > 0:
            gap = abs(ma20 - ma60) / ma60
            # MA20과 MA60 격차 2% 이내 → 수렴 중 (전환 임박)
            if gap <= 0.02:
                is_transition = True
                alignment = "수렴중"

    return is_transition, alignment, gc


def _check_abcd_c_point(df: pd.DataFrame) -> bool:
    """
    ABCD 패턴 'C' 포인트 감지:
    - 20일 고점 대비 5~15% 조정
    - MACD 히스토그램이 음 → 양 전환 (반전)
    """
    if len(df) < 25 or "MACD_Hist" not in df.columns:
        return False

    close = df["종가"].iloc[-1]
    high_20 = df["종가"].iloc[-25:-1].max()
    drawdown = (close - high_20) / high_20 * 100 if high_20 > 0 else 0

    # 2~25% 조정 구간 (완화)
    if not (-25 <= drawdown <= -2):
        return False

    # MACD 히스토그램 음→양 전환 또는 하락 둔화 (최근 5일 이내)
    hist = df["MACD_Hist"].values
    for i in range(-5, 0):
        if len(hist) + i >= 1:
            # 음→양 전환
            if hist[i] >= 0 and hist[i - 1] < 0:
                return True
            # 하락 둔화 (음수이지만 이전보다 절대값 감소)
            if hist[i] < 0 and hist[i - 1] < 0 and hist[i] > hist[i - 1]:
                return True
    return False


def _fetch_pbr(ticker: str) -> float:
    """PBR 값 가져오기. 실패 시 -1 반환."""
    try:
        fund = get_stock_fundamentals(ticker)
        pbr_val = fund.get("PBR", "")
        return _safe_float(pbr_val, -1.0)
    except Exception:
        return -1.0


# ═══════════════════════════════════════════════════════════════════════════
# 메인 스크리닝 함수
# ═══════════════════════════════════════════════════════════════════════════

def screen_knee_stocks(daily_df: pd.DataFrame, date: str,
                       max_results: int = 10) -> List[Dict]:
    """
    "무릎 아래" 저평가 가치주 스크리닝 메인 함수.

    1) daily_df에서 수급/등락률로 후보 사전필터 (API 호출 ✗)
    2) 후보별 OHLCV → 기술 지표 검증 (RSI 회복, 볼린저 반등, 배열 전환)
    3) 기술 지표 통과 종목만 PBR 확인 (API 호출 최소화)
    4) 종합 점수 계산 → 상위 반환
    """
    candidates = _prefilter_candidates(daily_df)
    if candidates.empty:
        return []

    results = []
    pbr_check_count = 0

    for ticker, row in candidates.iterrows():
        # API 부하 완화
        if pbr_check_count >= 50:
            break

        # ── OHLCV + 기술 지표 ──
        ohlcv = _fetch_ohlcv(str(ticker), date)
        if ohlcv is None:
            continue
        ohlcv = calc_all_indicators(ohlcv)

        # ── 기술 지표 체크 ──
        score = 0.0
        reasons = []

        # RSI 과매도 탈출
        rsi_ok, cur_rsi, min_rsi = _check_rsi_recovery(ohlcv)
        if rsi_ok:
            score += 20
            reasons.append(f"RSI {min_rsi:.0f}→{cur_rsi:.0f} 회복")

        # 볼린저 하단 반등
        bb_ok, cur_pctb = _check_bollinger_bounce(ohlcv)
        if bb_ok:
            score += 20
            reasons.append(f"BB 하단반등 (%B={cur_pctb:.2f})")

        # 이평 배열 전환
        align_ok, alignment, gc = _check_alignment_transition(ohlcv)
        if align_ok:
            score += 20
            label = "골든크로스" if gc else alignment
            reasons.append(f"배열전환({label})")

        # ABCD 'C' 포인트
        abcd_ok = _check_abcd_c_point(ohlcv)
        if abcd_ok:
            score += 15
            reasons.append("ABCD C포인트")

        # 최소 1개 이상 시그널 충족 시 PBR 확인
        signal_count = sum([rsi_ok, bb_ok, align_ok, abcd_ok])
        if signal_count < 1:
            continue

        # ── PBR 확인 (기술 지표 통과 종목만) ──
        pbr = _fetch_pbr(str(ticker))
        pbr_check_count += 1
        time.sleep(0.12)  # API 부하 완화

        if pbr < 0:
            # PBR 조회 실패 시 시그널 2개 이상이면 PBR 없이 포함
            if signal_count >= 2:
                pbr = 0.0
                reasons.append("PBR 미확인")
            else:
                continue
        elif not (0.2 <= pbr <= 1.5):
            continue  # PBR 범위 초과
        else:
            # PBR 스코어 (0.6~1.0 최적, 범위 밖이면 감점)
            if 0.6 <= pbr <= 1.0:
                score += 25
                reasons.append(f"PBR {pbr:.2f} (적정)")
            elif 0.2 <= pbr < 0.6:
                score += 15
                reasons.append(f"PBR {pbr:.2f} (심저평가)")
            elif 1.0 < pbr <= 1.2:
                score += 10
                reasons.append(f"PBR {pbr:.2f} (경계)")
            else:  # 1.2 < pbr <= 1.5
                score += 5
                reasons.append(f"PBR {pbr:.2f} (보통)")

        # 기관+외인 쌍끌이 보너스
        inst_5 = _safe_float(row.get("기관합계_5일", 0))
        frgn_5 = _safe_float(row.get("외국인합계_5일", 0))
        if inst_5 > 0 and frgn_5 > 0:
            score += 10
            reasons.append("기관+외인 쌍끌이")

        # ABCD + 배열전환 콤보 보너스
        if abcd_ok and align_ok:
            score += 10

        name = str(row.get("종목명", ticker))
        sector = row.get("업종", "")
        if not isinstance(sector, str):
            sector = ""

        results.append({
            "ticker": str(ticker),
            "name": name,
            "price": _safe_float(row.get("종가", 0)),
            "change": _safe_float(row.get("등락률", 0)),
            "volume_amt": _safe_float(row.get("거래대금", 0)),
            "sector": sector,
            "pbr": pbr,
            "rsi": round(cur_rsi, 1),
            "bb_pctb": round(cur_pctb, 2),
            "alignment": alignment,
            "golden_cross": gc,
            "abcd_c": abcd_ok,
            "inst_5d": inst_5,
            "frgn_5d": frgn_5,
            "score": round(score, 1),
            "reasons": reasons,
            "signal_count": signal_count,
        })

    # 점수 내림차순 정렬
    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:max_results]
