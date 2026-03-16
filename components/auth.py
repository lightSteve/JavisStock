"""
🔐 간단 로그인 모듈 (닉네임 + 비밀번호)
- 회원가입: 닉네임 + 비밀번호 → 해시 저장
- 로그인: 닉네임 + 비밀번호 확인
- 비밀번호는 SHA-256 해시로 저장 (원문 저장 없음)
- JSON 파일 기반 (users.json)
"""

import hashlib
import json
import os
import re
import streamlit as st

_AUTH_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "auth_data",
)
_USERS_FILE = os.path.join(_AUTH_DIR, "users.json")


def _ensure_dir():
    os.makedirs(_AUTH_DIR, exist_ok=True)


def _hash_password(password: str) -> str:
    """비밀번호를 SHA-256 해시로 변환."""
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def _load_users() -> dict:
    """사용자 목록 로드. {닉네임: {password_hash, created_at}}"""
    if os.path.exists(_USERS_FILE):
        try:
            with open(_USERS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def _save_users(users: dict):
    """사용자 목록 저장."""
    _ensure_dir()
    with open(_USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=2)


def _sanitize_username(name: str) -> str:
    """파일명에 안전한 닉네임으로 정규화."""
    return re.sub(r'[^\w가-힣]', '_', name.strip())[:20]


def is_logged_in() -> bool:
    """현재 로그인 상태 확인."""
    return bool(st.session_state.get("username"))


def get_username() -> str:
    """현재 로그인한 사용자 닉네임 반환."""
    return st.session_state.get("username", "")


def render_login_sidebar():
    """사이드바에 로그인/회원가입 UI 렌더링.

    로그인 성공 시 st.session_state["username"]에 닉네임 저장.
    """
    st.sidebar.markdown("### 🔐 로그인")

    # 이미 로그인 상태면 환영 메시지 + 로그아웃
    if is_logged_in():
        username = get_username()
        st.sidebar.markdown(
            f'<div style="background:linear-gradient(135deg,#059669,#10b981); '
            f'border-radius:10px; padding:10px 14px; margin-bottom:8px;">'
            f'<div style="color:#fff; font-weight:700; font-size:0.9em;">'
            f'👤 {username} 님</div>'
            f'<div style="color:#d1fae5; font-size:0.75em;">로그인 중</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        if st.sidebar.button("🚪 로그아웃", key="btn_logout", use_container_width=True):
            st.session_state.pop("username", None)
            st.rerun()
        return

    # 로그인 / 회원가입 탭
    login_tab, register_tab = st.sidebar.tabs(["로그인", "회원가입"])

    with login_tab:
        login_name = st.text_input(
            "닉네임", placeholder="닉네임", key="login_name",
        )
        login_pw = st.text_input(
            "비밀번호", type="password", placeholder="비밀번호", key="login_pw",
        )
        if st.button("✅ 로그인", key="btn_login", use_container_width=True, type="primary"):
            if not login_name or not login_pw:
                st.error("닉네임과 비밀번호를 입력해주세요.")
            else:
                safe_name = _sanitize_username(login_name)
                users = _load_users()
                if safe_name not in users:
                    st.error("등록되지 않은 닉네임입니다.")
                elif users[safe_name]["password_hash"] != _hash_password(login_pw):
                    st.error("비밀번호가 틀렸습니다.")
                else:
                    st.session_state["username"] = safe_name
                    st.success(f"✅ {safe_name} 님 환영합니다!")
                    st.rerun()

    with register_tab:
        reg_name = st.text_input(
            "닉네임", placeholder="사용할 닉네임", key="reg_name",
        )
        reg_pw = st.text_input(
            "비밀번호", type="password", placeholder="비밀번호 (4자 이상)", key="reg_pw",
        )
        reg_pw2 = st.text_input(
            "비밀번호 확인", type="password", placeholder="비밀번호 재입력", key="reg_pw2",
        )
        if st.button("📝 회원가입", key="btn_register", use_container_width=True):
            if not reg_name or not reg_pw:
                st.error("닉네임과 비밀번호를 입력해주세요.")
            elif len(reg_pw) < 4:
                st.error("비밀번호는 4자 이상이어야 합니다.")
            elif reg_pw != reg_pw2:
                st.error("비밀번호가 일치하지 않습니다.")
            else:
                safe_name = _sanitize_username(reg_name)
                if not safe_name:
                    st.error("유효한 닉네임을 입력해주세요.")
                else:
                    users = _load_users()
                    if safe_name in users:
                        st.error(f"'{safe_name}'은(는) 이미 사용중인 닉네임입니다.")
                    else:
                        from datetime import datetime
                        users[safe_name] = {
                            "password_hash": _hash_password(reg_pw),
                            "created_at": datetime.now().isoformat(),
                        }
                        _save_users(users)
                        st.session_state["username"] = safe_name
                        st.success(f"🎉 가입 완료! {safe_name} 님 환영합니다!")
                        st.rerun()
