"""
Supabase REST API 헬퍼 — users / portfolios / journals 테이블 CRUD.
파일 기반 저장을 대체하여 Streamlit Cloud 재배포 시에도 데이터가 유지됩니다.
"""

import streamlit as st
import requests
from typing import Any


def _cfg():
    """(url, key) 반환. Secrets 미설정 시 (None, None)."""
    try:
        url = st.secrets["supabase"]["url"].rstrip("/")
        key = st.secrets["supabase"]["anon_key"]
        return url, key
    except Exception:
        return None, None


def _headers():
    _, key = _cfg()
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def is_configured() -> bool:
    url, key = _cfg()
    return bool(url and key)


# ─────────────────────────────────────────────────────────────────────
# users
# ─────────────────────────────────────────────────────────────────────

def get_user(username: str) -> dict | None:
    url, _ = _cfg()
    if not url:
        return None
    r = requests.get(
        f"{url}/rest/v1/users",
        headers=_headers(),
        params={"username": f"eq.{username}", "select": "*"},
        timeout=10,
    )
    rows = r.json() if r.ok else []
    return rows[0] if rows else None


def upsert_user(username: str, password_hash: str, created_at: str):
    url, _ = _cfg()
    if not url:
        return
    requests.post(
        f"{url}/rest/v1/users",
        headers={**_headers(), "Prefer": "resolution=merge-duplicates,return=minimal"},
        json={"username": username, "password_hash": password_hash, "created_at": created_at},
        timeout=10,
    )


# ─────────────────────────────────────────────────────────────────────
# portfolios
# ─────────────────────────────────────────────────────────────────────

def load_portfolio(username: str) -> list:
    url, _ = _cfg()
    if not url:
        return []
    r = requests.get(
        f"{url}/rest/v1/portfolios",
        headers=_headers(),
        params={"username": f"eq.{username}", "select": "data"},
        timeout=10,
    )
    rows = r.json() if r.ok else []
    if rows and isinstance(rows[0].get("data"), list):
        return rows[0]["data"]
    return []


def save_portfolio(username: str, data: list):
    url, _ = _cfg()
    if not url:
        return
    requests.post(
        f"{url}/rest/v1/portfolios",
        headers={**_headers(), "Prefer": "resolution=merge-duplicates,return=minimal"},
        json={"username": username, "data": data, "updated_at": _now()},
        timeout=10,
    )


# ─────────────────────────────────────────────────────────────────────
# journals
# ─────────────────────────────────────────────────────────────────────

def load_journal(username: str) -> list:
    url, _ = _cfg()
    if not url:
        return []
    r = requests.get(
        f"{url}/rest/v1/journals",
        headers=_headers(),
        params={"username": f"eq.{username}", "select": "data"},
        timeout=10,
    )
    rows = r.json() if r.ok else []
    if rows and isinstance(rows[0].get("data"), list):
        return rows[0]["data"]
    return []


def save_journal(username: str, data: list):
    url, _ = _cfg()
    if not url:
        return
    requests.post(
        f"{url}/rest/v1/journals",
        headers={**_headers(), "Prefer": "resolution=merge-duplicates,return=minimal"},
        json={"username": username, "data": data, "updated_at": _now()},
        timeout=10,
    )


def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────
# market_snapshots  (전종목 일별 OHLCV + 수급 스냅샷)
# ─────────────────────────────────────────────────────────────────────
# Supabase SQL (최초 1회 실행):
#   CREATE TABLE IF NOT EXISTS market_snapshots (
#       date    TEXT NOT NULL,
#       market  TEXT NOT NULL DEFAULT 'ALL',
#       csv_data TEXT NOT NULL,
#       updated_at TEXT,
#       PRIMARY KEY (date, market)
#   );
# ─────────────────────────────────────────────────────────────────────

def save_market_snapshot(date: str, market: str, df) -> bool:
    """market_snapshots 테이블에 전종목 스냅샷 upsert.

    Args:
        date:   'YYYYMMDD'
        market: 'ALL' 등
        df:     pandas DataFrame (index=티커)

    Returns:
        True on success, False otherwise.
    """
    url, _ = _cfg()
    if not url:
        return False
    try:
        import io
        buf = io.StringIO()
        df.to_csv(buf)
        csv_str = buf.getvalue()
        r = requests.post(
            f"{url}/rest/v1/market_snapshots",
            headers={**_headers(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json={"date": date, "market": market, "csv_data": csv_str, "updated_at": _now()},
            timeout=60,
        )
        return r.ok
    except Exception:
        return False


def load_market_snapshot(date: str, market: str):
    """market_snapshots 테이블에서 스냅샷 로드.

    Returns:
        pandas DataFrame (index=티커), 없으면 빈 DataFrame.
    """
    url, _ = _cfg()
    if not url:
        import pandas as pd
        return pd.DataFrame()
    try:
        import pandas as pd, io
        r = requests.get(
            f"{url}/rest/v1/market_snapshots",
            headers=_headers(),
            params={"date": f"eq.{date}", "market": f"eq.{market}", "select": "csv_data"},
            timeout=30,
        )
        rows = r.json() if r.ok else []
        if not rows or not rows[0].get("csv_data"):
            return pd.DataFrame()
        return pd.read_csv(io.StringIO(rows[0]["csv_data"]), index_col=0)
    except Exception:
        import pandas as pd
        return pd.DataFrame()


def list_market_snapshots(market: str = "ALL") -> list:
    """Supabase에 저장된 스냅샷 일자 목록 반환."""
    url, _ = _cfg()
    if not url:
        return []
    try:
        r = requests.get(
            f"{url}/rest/v1/market_snapshots",
            headers=_headers(),
            params={"market": f"eq.{market}", "select": "date", "order": "date.desc"},
            timeout=10,
        )
        rows = r.json() if r.ok else []
        return [row["date"] for row in rows if "date" in row]
    except Exception:
        return []
