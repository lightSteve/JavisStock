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
