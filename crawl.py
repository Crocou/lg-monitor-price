# crawl_scroll_zip65760.py
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위
- LG 모니터만 필터, 가격·순위·변동 기록 (스크롤 포함)
- 배송지(우편번호) 65760 고정
"""

import os, re, json, base64, datetime, time, pandas as pd, requests, gspread, pytz
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By

# ────────────────────────── 1. Selenium 준비 ──────────────────────────
def get_driver():
    opt = webdriver.ChromeOptions()
    opt.add_argument("--headless=new")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--window-size=1280,4000")
    opt.add_argument("--lang=de-DE")
    opt.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    )
    return webdriver.Chrome(options=opt)

# ★ 1-A. 우편번호 고정 -------------------------------------------------------------
def set_zip(driver, zip_code: str = "65760"):
    """AJAX 호출 한 번으로 배송지 우편번호를 고정한다."""
    payload = (
        f"locationType=LOCATION_INPUT&zipCode={zip_code}"
        "&storeContext=computers&deviceType=web&pageType=Detail&actionSource=glow"
    )
    script = """
        const body = arguments[0], done = arguments[1];
        fetch("https://www.amazon.de/gp/delivery/ajax/address-change.html", {
            method: "POST",
            headers: {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest"
            },
            body
        }).finally(done);
    """
    driver.execute_async_script(script, payload)
    driver.refresh()
    time.sleep(1)  # 쿠키 적용 대기
# ----------------------------------------------------------------------

BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"  # pg=1|2
CARD_SELECTOR = (
    "li.zg-no-numbers, "                               # 데스크톱 베스트셀러
    "div.p13n-sc-uncoverable-faceout, "
    "div.zg-grid-general-faceout, "
    "li[data-asin]"                                    # 속성 기반 백업
)

# ────────────────────────── 2. 페이지 가져오기 ──────────────────────────
def fetch_cards(page: int, driver):
    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
    driver.get(url)

    # 언어·통화 쿠키 강제
    driver.add_cookie({"name": "lc-main", "value": "de_DE"})
    driver.add_cookie({"name": "i18n-prefs", "value": "EUR"})
    driver.refresh()

    # 스크롤 끝까지 내려 새 카드 로드
    SCROLL_PAUSE = 6
    last_count = 0
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)
        cards_now = driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)
        if len(cards_now) == last_count:
            break
        last_count = len(cards_now)

    # CAPTCHA 체크
    html = driver.page_source
    if "Enter the characters you see below" in html or "Type the characters" in html:
        raise RuntimeError("Amazon CAPTCHA!")

    soup = BeautifulSoup(html, "lxml")
    return soup.select(CARD_SELECTOR)

# ────────────────────────── 3. 개별 파서 ──────────────────────────
def pick_title(card):
    for sel in [
        'span[class*="p13n-sc-css-line-clamp"]',
        "[title]",
        ".p13n-sc-truncate-desktop-type2",
        ".zg-text-center-align span.a-size-base",
    ]:
        t = card.select_one(sel)
        if t:
            return t.get("title", "") if sel == "[title]" else t.get_text(strip=True)
    img = card.select_one("img")
    return img.get("alt", "").strip() if img else ""

def pick_price(card):
    for sel in [
        "span.p13n-sc-price",
        "span.a-size-base.a-color-price",
        "span.a-color-price",
        "span.a-price > span.a-offscreen",
    ]:
        p = card.select_one(sel)
        if p:
            return p.get_text(strip=True)
    # whole + fraction 백업
    whole = card.select_one("span.a-price-whole")
    frac  = card.select_one("span.a-price-fraction")
    if whole:
        txt = whole.get_text(strip=True).replace(".", "").replace(",", ".")
        if frac:
            txt += frac.get_text(strip=True)
        return txt
    return ""

def pick_rank(card):
    badge = card.select_one("span.zg-badge-text")
    if badge and badge.text.strip("#").isdigit():
        return int(badge.text.strip("#"))
    li = card.find_parent("li", attrs={"data-index": True})
    if li and li["data-index"].isdigit():
        return int(li["data-index"]) + 1
    return None

# ────────────────────────── 4. 크롤링 실행 ──────────────────────────
driver = get_driver()
driver.get("https://www.amazon.de/")
set_zip(driver, "65760")

cards = []
for pg in (1, 2):
    cards.extend(fetch_cards(pg, driver))
print(f"[INFO] total cards fetched after scroll: {len(cards)}")
if not cards:
    driver.quit()
    raise RuntimeError("✖️ 카드 0개 – 셀렉터 확인 필요")

items = []
for card in cards:
    a = card.select_one("a.a-link-normal[href*='/dp/']")
    if not a:
        continue

    title = pick_title(card) or a.get_text(" ", strip=True)
    if not re.search(r"\bLG\b", title, re.I):
        continue  # LG 모니터만

    rank_val  = pick_rank(card)
    price_txt = pick_price(card)
    if rank_val is None or not price_txt:
        continue

    link = "https://www.amazon.de" + a["href"].split("?", 1)[0]
    asin = re.search(r"/dp/([A-Z0-9]{10})", link).group(1)

    items.append(
        {
            "asin":  asin,
            "title": title,
            "url":   link,
            "price": price_txt,
            "rank":  rank_val,
        }
    )

driver.quit()
if not items:
    raise RuntimeError("✖️ LG 모니터를 찾지 못했습니다.")

df_today = pd.DataFrame(items).sort_values("rank").reset_index(drop=True)

# ────────────────────────── 5. 날짜 추가 ──────────────────────────
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

ws_hist = (sh.worksheet("History")
           if "History" in [w.title for w in sh.worksheets()]
           else sh.add_worksheet("History", rows=2000, cols=20))
ws_today = (sh.worksheet("Today")
            if "Today"   in [w.title for w in sh.worksheets()]
            else sh.add_worksheet("Today", rows=100,  cols=20))

# 6-A. Δ 계산
try:
    prev = pd.DataFrame(ws_hist.get_all_records()).dropna()
except gspread.exceptions.APIError:
    prev = pd.DataFrame()

if not prev.empty and {"asin", "rank", "price", "date"} <= set(prev.columns):
    latest = (prev.sort_values("date")
                  .groupby("asin", as_index=False)
                  .last()[["asin", "rank", "price"]]
                  .rename(columns={"rank": "rank_prev", "price": "price_prev"}))
    df_today = df_today.merge(latest, on="asin", how="left")
else:
    df_today["rank_prev"]  = None
    df_today["price_prev"] = None

for col in ["price", "price_prev", "rank_prev"]:
    df_today[col] = pd.to_numeric(df_today[col], errors="coerce")

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

# 6-B. 시트 쓰기
if not ws_hist.get_all_values():
    ws_hist.append_row(cols, value_input_option="USER_ENTERED")
ws_hist.append_rows(df_today.values.tolist(), value_input_option="USER_ENTERED")

ws_today.clear()
ws_today.update([cols] + df_today.values.tolist(), value_input_option="USER_ENTERED")

# 6-C. Δ 서식(빨강·파랑)
from gspread_formatting import format_cell_ranges, CellFormat, TextFormat, Color
RED, BLUE = Color(1, 0, 0), Color(0, 0, 1)
delta_cols = {"rank_delta": "G", "price_delta": "H"}

fmt_ranges = []
for i, row in df_today.iterrows():
    r = i + 2
    for col_name, col_letter in delta_cols.items():
        val = row[col_name]
        if isinstance(val, str) and val.startswith("▲"):
            fmt_ranges.append((f"{col_letter}{r}",
                               CellFormat(textFormat=TextFormat(bold=True, foregroundColor=RED))))
        elif isinstance(val, str) and val.startswith("▼"):
            fmt_ranges.append((f"{col_letter}{r}",
                               CellFormat(textFormat=TextFormat(bold=True, foregroundColor=BLUE))))

if fmt_ranges:
    format_cell_ranges(ws_today, fmt_ranges)

print("✓ Google Sheet 업데이트 완료 — LG 모니터", len(df_today))
