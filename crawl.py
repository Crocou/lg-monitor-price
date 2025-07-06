# crawl_scroll_zip65760.py
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위
- LG 모니터 필터, 가격·순위·변동 기록 (스크롤 포함)
- ★ 배송지(우편번호) 65760 고정
"""

import os, re, json, base64, datetime, time, requests, pandas as pd, gspread, pytz
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# ────────────────────────── 1. Selenium 준비 ──────────────────────────
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

def get_driver():
    service = None
    opt = webdriver.ChromeOptions()
    opt.add_argument("--headless=new")          # CI/서버용
    opt.add_argument("--no-sandbox")            # GitHub Actions 권장
    opt.add_argument("--disable-dev-shm-usage") # /dev/shm 용량 문제 방지
    opt.add_argument("--window-size=1280,4000")
    opt.add_argument("--lang=de-DE")
    opt.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0 Safari/537.36"
    )
    return webdriver.Chrome(service=service, options=opt)

# ★ 1-A. 우편번호 고정 함수 --------------------------------------------------------
def set_zip(driver, zip_code="65760"):
    """
    현재 세션에 배송지 ZIP 코드를 고정한다.
    Amazon의 address-change AJAX 엔드포인트에 한 번 POST → 쿠키·세션 갱신
    """
    payload = (
        f"locationType=LOCATION_INPUT&zipCode={zip_code}"
        "&storeContext=computers&deviceType=web&pageType=Detail&actionSource=glow"
    )
    script = """
        const zip = arguments[0];
        const body = arguments[1];
        const done = arguments[2];

        fetch("https://www.amazon.de/gp/delivery/ajax/address-change.html", {
            method: "POST",
            headers: {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest"
            },
            body: body
        })
        .then(() => done())
        .catch(() => done());
    """
    driver.execute_async_script(script, zip_code, payload)
    driver.refresh()               # 새 쿠키 기준으로 페이지 재로드
    time.sleep(1)                  # 쿠키 적용까지 1초 여유
# --------------------------------------------------------------------------

BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"  # pg=1|2

# ────────────────────────── 2. 페이지 가져오기 (스크롤 포함) ──────────────────────────
def fetch_page_soup(page: int, driver):
    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
    driver.get(url)

    # ★ 2-A. Amazon 쿠키(언어/통화) 강제 (우편번호와 별개)
    driver.add_cookie({"name": "lc-main", "value": "de_DE"})
    driver.add_cookie({"name": "i18n-prefs", "value": "EUR"})
    driver.refresh()

    # ★ 2-B. 스크롤 : 새 아이템이 더 이상 증가하지 않을 때까지
    SCROLL_PAUSE = 30
    last_count = 0
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)
         cards_now = driver.find_elements(By.CSS_SELECTOR, "li.zg-no-numbers")
        if len(cards_now) == last_count:
            break
        last_count = len(cards_now)

    # ★ 2-C. BeautifulSoup 변환
    html = driver.page_source
    if "Enter the characters you see below" in html:
        raise RuntimeError("Amazon CAPTCHA!")
    soup = BeautifulSoup(html, "lxml")
    return soup.select("div.zg-grid-general-faceout") or soup.select("div.p13n-sc-uncoverable-faceout")

# ────────────────────────── 3. 전체 1-100위 카드 수집 ──────────────────────────
driver = get_driver()

# ★ 3-A. 세션 시작 시 ZIP 고정 (한 번만 호출)
driver.get("https://www.amazon.de/")   # 아무 페이지나 먼저 접속
set_zip(driver, "65760")               # ← 우편번호 고정

cards = []
for pg in (1, 2):
    cards += fetch_page_soup(pg, driver)
driver.quit()
print(f"[INFO] total cards fetched after scroll: {len(cards)}")  # 기대값 ≈ 100

# ────────────────────────── 4. 이후 로직은 기존 코드 재사용 ──────────────────────────
def pick_title(card):
    for sel in [
        'span[class*="p13n-sc-css-line-clamp"]',
        '[title]',
        '.p13n-sc-truncate-desktop-type2',
        '.zg-text-center-align span.a-size-base',
    ]:
        t = card.select_one(sel)
        if t:
            txt = t.get("title", "") if sel == "[title]" else t.get_text(strip=True)
            if txt:
                return txt
    img = card.select_one("img")
    return img.get("alt", "").strip() if img else ""

def pick_price(card):
    p = card.select_one("span.p13n-sc-price")
    if p:
        return p.get_text(strip=True)

items = []
for idx, card in enumerate(cards, start=1):       # ★ idx == 실제 랭킹 (1~100)
    a = card.select_one("a.a-link-normal[href*='/dp/']")
    if not a:
        continue
    title = pick_title(card) or a.get_text(" ", strip=True)
    if not re.search(r"\bLG\b", title, re.I):
        continue                                     # LG 필터
    link = "https://www.amazon.de" + a["href"].split("?", 1)[0]
    asin = re.search(r"/dp/([A-Z0-9]{10})", link).group(1)
    price_val = pick_price(card)
    items.append(
        {
            "asin": asin,
            "title": title,
            "url": link,
            "price": price_val,
            "rank": idx,            # ★ 스크롤 덕분에 정확
        }
    )

df_today = pd.DataFrame(items).sort_values("rank").reset_index(drop=True)

# ────────────────────────── 5. 날짜 및 Google Sheet 기록 (기존 그대로) ──────────────────────────
kst = pytz.timezone("Asia/Seoul")
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

# ────────────────────────── 6. Google Sheet 기록 ──────────────────────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(
    json.loads(base64.b64decode(os.environ["GCP_SA_BASE64"]).decode()),
    scopes=SCOPES,
)
gc = gspread.authorize(creds)

SHEET_ID = os.environ["SHEET_ID"]
sh = gc.open_by_key(SHEET_ID)

# 워크시트 핸들 확보(없으면 생성)
ws_hist = (
    sh.worksheet("History")
    if "History" in [w.title for w in sh.worksheets()]
    else sh.add_worksheet("History", rows=2000, cols=20)
)
ws_today = (
    sh.worksheet("Today")
    if "Today" in [w.title for w in sh.worksheets()]
    else sh.add_worksheet("Today", rows=100, cols=20)
)

# ────────────────── 6-A. 이전 History 불러와서 delta 계산 ──────────────────
try:
    prev = pd.DataFrame(ws_hist.get_all_records()).dropna()
except gspread.exceptions.APIError:
    prev = pd.DataFrame()

if not prev.empty and {"asin", "rank", "price", "date"} <= set(prev.columns):
    latest = (
        prev.sort_values("date")
            .groupby("asin", as_index=False)
            .last()[["asin", "rank", "price"]]
            .rename(columns={"rank": "rank_prev", "price": "price_prev"})
    )
    df_today = df_today.merge(latest, on="asin", how="left")
else:
    df_today["rank_prev"] = None
    df_today["price_prev"] = None

# 숫자형 컬럼 변환 (문자열·빈값 → NaN)
for col in ["price", "price_prev", "rank_prev"]:
    df_today[col] = pd.to_numeric(df_today[col], errors="coerce")

# Δ 계산
df_today["rank_delta_num"]  = df_today["rank_prev"]  - df_today["rank"]
df_today["price_delta_num"] = df_today["price"]      - df_today["price_prev"]

def fmt(val, is_price=False):
    if pd.isna(val) or val == 0:
        return "-"
    arrow = "▲" if val > 0 else "▼"
    return f"{arrow}{abs(val):.2f}" if is_price else f"{arrow}{abs(int(val))}"

df_today["rank_delta"]  = df_today["rank_delta_num"].apply(fmt)
df_today["price_delta"] = df_today["price_delta_num"].apply(lambda x: fmt(x, True))

cols = ["asin", "title", "rank", "price", "url", "date",
        "rank_delta", "price_delta"]
df_today = df_today[cols].fillna("")

# ────────────────── 6-B. 시트 쓰기 ──────────────────
# History: 헤더가 없으면 추가 후, 행 단위 append
if not ws_hist.get_all_values():
    ws_hist.append_row(cols, value_input_option="USER_ENTERED")
ws_hist.append_rows(df_today.values.tolist(), value_input_option="USER_ENTERED")

# Today: 기존 내용 지우고 새로 쓰기
ws_today.clear()
ws_today.update([cols] + df_today.values.tolist(), value_input_option="USER_ENTERED")

# ────────────────── 6-C. ▲ / ▼ 서식 (빨강·파랑 + 볼드) ──────────────────
from gspread_formatting import (
    format_cell_ranges,
    CellFormat,
    TextFormat,
    Color,
)

RED  = Color(1, 0, 0)   # RGB  (1,0,0) = 빨강
BLUE = Color(0, 0, 1)   # RGB  (0,0,1) = 파랑

delta_cols = {"rank_delta": "G", "price_delta": "H"}   # Today 시트 열 위치에 맞게 수정

fmt_ranges = []
for i, row in df_today.iterrows():
    r = i + 2  # 헤더 다음부터 시작
    for col_name, col_letter in delta_cols.items():
        val = row[col_name]
        if isinstance(val, str) and val.startswith("▲"):
            fmt_ranges.append((f"{col_letter}{r}", CellFormat(textFormat=TextFormat(bold=True, foregroundColor=RED))))
        elif isinstance(val, str) and val.startswith("▼"):
            fmt_ranges.append((f"{col_letter}{r}", CellFormat(textFormat=TextFormat(bold=True, foregroundColor=BLUE))))

if fmt_ranges:
    format_cell_ranges(ws_today, fmt_ranges)

print("✓ Google Sheet 업데이트 완료 — LG 모니터", len(df_today))
