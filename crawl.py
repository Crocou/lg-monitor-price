import os, re, json, datetime, requests, pandas as pd, gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# ────────────────────────────────────────────────────────────────
# 1) 아마존 베스트셀러 스크랩
# ────────────────────────────────────────────────────────────────
URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
}
resp = requests.get(URL, headers=HEADERS, timeout=30)
resp.raise_for_status()

soup = BeautifulSoup(resp.text, "lxml")
items = []
for li in soup.select("ol#zg-ordered-list > li"):
    rank_tag  = li.select_one(".zg-badge-text")
    title_tag = li.select_one("img")
    link_tag  = li.select_one("a.a-link-normal")

    if not (rank_tag and title_tag and link_tag):
        continue

    rank  = int(rank_tag.text.strip("#"))
    title = title_tag["alt"].strip()
    link  = "https://www.amazon.de" + link_tag["href"].split("?", 1)[0]
    asin_match = re.search(r"/dp/([A-Z0-9]{10})", link)
    asin = asin_match.group(1) if asin_match else None

    if "LG" in title.upper():          # LG 모니터만 필터
        items.append({"asin": asin, "title": title, "rank": rank, "url": link})

if not items:
    raise RuntimeError("LG 모니터가 목록에 없습니다!")

df_today = pd.DataFrame(items).sort_values("rank").reset_index(drop=True)
df_today["date"] = datetime.date.today().isoformat()

# ────────────────────────────────────────────────────────────────
# 2) Google Sheets 연결
# ────────────────────────────────────────────────────────────────
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.environ["1ReBV9KK0LEanYHguhVR8VT167wY1Gvwa8MDzIN2Lr-s"]

creds = Credentials.from_service_account_info(
            json.loads(os.environ["GCP_SA_JSON"]), scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)

def get_or_create(name, rows=1000, cols=20):
    try:
        return sh.worksheet(name)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(name, rows, cols)

ws_hist = get_or_create("History")
ws_today = get_or_create("Today")

# ────────────────────────────────────────────────────────────────
# 3) 변동률(Δ) 계산
# ────────────────────────────────────────────────────────────────
try:
    prev = pd.DataFrame(ws_hist.get_all_records()).dropna()
except gspread.exceptions.APIError:
    prev = pd.DataFrame()

if not prev.empty:
    latest_prev = (prev.sort_values("date")
                        .groupby("asin", as_index=False)
                        .last()[["asin", "rank"]]
                        .rename(columns={"rank": "rank_prev"}))
    df_today = df_today.merge(latest_prev, on="asin", how="left")
    df_today["delta"] = df_today["rank_prev"] - df_today["rank"]
else:
    df_today["delta"] = None

# ────────────────────────────────────────────────────────────────
# 4) 시트 업데이트
# ────────────────────────────────────────────────────────────────
# 4-1) History 탭: append
ws_hist.append_rows(df_today.values.tolist(), value_input_option="RAW")

# 4-2) Today 탭: 덮어쓰기
ws_today.clear()
ws_today.update([df_today.columns.values.tolist()] + df_today.values.tolist(),
                value_input_option="RAW")
print("✓ Sheet updated")
