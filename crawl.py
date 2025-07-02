import os, json, requests, pandas as pd, gspread
from google.oauth2.service_account import Credentials

# 1) ---- 가격 가져오기 (예시는 네이버 오픈API; 나중에 다나와 등 추가) ----
NAVER_ID     = os.environ["NAVER_ID"]
NAVER_SECRET = os.environ["NAVER_SECRET"]
resp = requests.get(
    "https://openapi.naver.com/v1/search/shop.json",
    headers={"X-Naver-Client-Id": NAVER_ID, "X-Naver-Client-Secret": NAVER_SECRET},
    params={"query": "LG 모니터", "display": 100, "sort": "asc"},
).json()["items"]
df = (pd.DataFrame(resp)
        .assign(price=lambda d: d["lprice"].astype(int))
        .sort_values("price"))

# 2) ---- Google Sheets 쓰기 ----
SCOPES  = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.environ["SHEET_ID"]
creds = Credentials.from_service_account_info(
            json.loads(os.environ["GCP_SA_JSON"]), scopes=SCOPES)
gc = gspread.authorize(creds)
ws = gc.open_by_key(SHEET_ID).worksheet("Sheet1")   # 탭 이름 맞게 수정
ws.clear()
ws.update([df.columns.values.tolist()] + df.values.tolist(),
          value_input_option='RAW')
