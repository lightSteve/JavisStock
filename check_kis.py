#!/usr/bin/env python3
"""KIS API 실제 응답 확인 스크립트"""
import requests, json, sys

# ─── 아래 두 값을 실제 키로 교체하세요 ───
APP_KEY    = "여기에_app_key_입력"
APP_SECRET = "여기에_app_secret_입력"
TICKER     = "005930"  # 삼성전자로 테스트
# ─────────────────────────────────────────

BASE = "https://openapi.koreainvestment.com:9443"

# 1. 토큰 발급
print("=== 1. 토큰 발급 ===")
resp = requests.post(f"{BASE}/oauth2/tokenP", json={
    "grant_type": "client_credentials",
    "appkey": APP_KEY,
    "appsecret": APP_SECRET,
}, timeout=10)
print(f"Status: {resp.status_code}")
tok_data = resp.json()
print(json.dumps(tok_data, ensure_ascii=False, indent=2)[:300])
token = tok_data.get("access_token", "")
if not token:
    print("토큰 발급 실패!")
    sys.exit(1)
print(f"\n토큰 발급 성공: {token[:30]}...")

# 2. 주식현재가 투자자 조회 (FHKST01010900)
print("\n=== 2. 주식현재가 투자자 (FHKST01010900) ===")
resp2 = requests.get(
    f"{BASE}/uapi/domestic-stock/v1/quotations/inquire-investor",
    headers={
        "Authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "FHKST01010900",
        "custtype": "P",
    },
    params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": TICKER},
    timeout=10,
)
print(f"Status: {resp2.status_code}")
data = resp2.json()
print(f"rt_cd: {data.get('rt_cd')}  msg: {data.get('msg1', '')}")
print("\n전체 응답:")
print(json.dumps(data, ensure_ascii=False, indent=2)[:2000])

# 3. output 구조 전체 분석
print("\n=== 응답 최상위 키 ===")
for k in data.keys():
    print(f"  '{k}'")

for key in ["output", "output1", "output2"]:
    raw = data.get(key)
    if raw is None:
        print(f"\n[{key}]: 없음")
        continue
    if isinstance(raw, list):
        print(f"\n[{key}]: 리스트, 길이={len(raw)}")
        for i, item in enumerate(raw[:4]):  # 최대 4행 출력
            print(f"  --- 행[{i}] ---")
            if isinstance(item, dict):
                for k, v in item.items():
                    print(f"    {k}: {v}")
    elif isinstance(raw, dict):
        print(f"\n[{key}]: 단일 dict")
        for k, v in raw.items():
            print(f"  {k}: {v}")
    else:
        print(f"\n[{key}]: {type(raw).__name__} = {raw}")

print("\n=== 수급 파싱 결과 (fetcher 로직 적용) ===")
output = data.get("output") or data.get("output1")
if output:
    def _parse(s):
        v = str(s).replace(",", "").replace("+", "").strip()
        try: return float(v) / 1e8
        except: return 0.0
    if isinstance(output, dict):
        indv = output.get("prsn_ntby_tr_pbmn") or output.get("indv_ntby_tr_pbmn", "0")
        print(f"  포맷A(단일dict): 외국인={_parse(output.get('frgn_ntby_tr_pbmn','0')):+.4f}억, 기관={_parse(output.get('orgn_ntby_tr_pbmn','0')):+.4f}억, 개인={_parse(indv):+.4f}억")
    elif isinstance(output, list) and output:
        first = output[0]
        if "frgn_ntby_tr_pbmn" in first:
            indv = first.get("prsn_ntby_tr_pbmn") or first.get("indv_ntby_tr_pbmn", "0")
            print(f"  포맷B-1(리스트+통합필드): 외국인={_parse(first.get('frgn_ntby_tr_pbmn','0')):+.4f}억, 기관={_parse(first.get('orgn_ntby_tr_pbmn','0')):+.4f}억, 개인={_parse(indv):+.4f}억")
        else:
            def _rv(rows, i): return _parse(rows[i].get("ntby_tr_pbmn","0")) if i < len(rows) else 0.0
            print(f"  포맷B-2(유형별리스트): 개인[0]={_rv(output,0):+.4f}억, 외국인[1]={_rv(output,1):+.4f}억, 기관합계[2]={_rv(output,2):+.4f}억")
