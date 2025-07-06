# crawl_scroll_zip65760.py
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위
- LG 모니터 필터, 가격·순위·변동 기록 (스크롤 포함)
- ★ 배송지(우편번호) 65760 고정
"""

import sys, os, re, json, base64, datetime, time, logging
import requests, pandas as pd, gspread, pytz
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from gspread_formatting import format_cell_ranges, CellFormat, TextFormat, Color

# ─── 0. 로깅 설정 ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("crawl_cards.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logging.info("🔍 LG 모니터 크롤러 시작")

# ────────────────────────── 1. Selenium 준비 ──────────────────────────
def get_driver():
    service = None
    opt = webdriver.ChromeOptions()
    opt.add_argument("--headless=new")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-dev-shm-usage")
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
    driver.refresh()
    time.sleep(1)

BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"  # pg=1|2
CARD_SEL = (
    "li.zg-no-numbers"
)

# ────────────────────────── 2. 페이지에서 카드 가져오기 ──────────────────────────
def fetch_cards(page: int, driver):
    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
    driver.get(url)

    driver.add_cookie({"name": "lc-main", "value": "de_DE"})
    driver.add_cookie({"name": "i18n-prefs", "value": "EUR"})
    driver.refresh()

    SCROLL_PAUSE = 30
    last = 0
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)
        now = len(driver.find_elements(By.CSS_SELECTOR, CARD_SEL))
        if now == last:
            break
        last = now

    return driver.find_elements(By.CSS_SELECTOR, CARD_SEL)

# ────────────────────────── 3. 카드 파싱 보조 함수 ──────────────────────────
def pick_title(card):
    try:
        t = card.find_element(
            By.XPATH,
            './/div[contains(@class,"_cDEzb_p13n-sc-css-line-clamp-2_EWgCb")]',
        ).text.strip()
        if t:
            return t
    except NoSuchElementException:
        pass
    try:
        img = card.find_element(By.XPATH, './/img[@alt]')
        return img.get_attribute("alt").strip()
    except NoSuchElementException:
        return ""

def pick_price(card):
    try:
        return card.find_element(
            By.XPATH,
            './/span[contains(@class,"p13n-sc-price")]',
        ).text.strip()
    except NoSuchElementException:
        return ""

def pick_rank(card):
    r = card.find_element(
        By.XPATH,
        './/span[contains(@class,"zg-bdg-text")]',
    ).text
    return int(re.sub(r"\D", "", r))

def money_to_float(txt: str):
    val = re.sub(r"[^\d,\.]", "", txt).replace(".", "").replace(",", ".")
    try:
        return float(val)
    except ValueError:
        return None

# ────────────────────────── 4. 전체 카드 수집 및 로깅 ──────────────────────────
driver = get_driver()
driver.get("https://www.amazon.de/")
set_zip(driver, "65760")

cards = []
for pg in (1, 2):
    cards += fetch_cards(pg, driver)
logging.info(f"총 {len(cards)}개 카드 수집 완료")

items = []
for idx, card in enumerate(cards, start=1):
    try:
        rank = pick_rank(card)
    except (NoSuchElementException, ValueError):
        logging.warning(f"[{idx}] 랭크 추출 실패 → 건너뜀")
        continue

    title = pick_title(card)
    lg_match = bool(re.search(r"\bLG\b", title, re.I))
    price_raw = pick_price(card)
    price_val = money_to_float(price_raw)

    try:
        a = card.find_element(By.XPATH, './/a[contains(@href,"/dp/")]')
        link = "https://www.amazon.de" + a.get_attribute("href").split("?", 1)[0]
        asin = re.search(r"/dp/([A-Z0-9]{10})", link).group(1)
    except Exception:
        logging.warning(f"[{idx}] 링크/ASIN 추출 실패 → 건너뜀")
        continue

    # 카드 전체 정보 로그
    card_info = {
        "rank": rank,
        "title": title,
        "price_text": price_raw,
        "price": price_val,
        "asin": asin,
        "url": link,
        "lg_match": lg_match,
    }
    logging.info(f"CARD_DATA {json.dumps(card_info, ensure_ascii=False)}")

    if not lg_match:
        continue

    items.append(
        {"asin": asin, "title": title, "url": link, "price": price_val, "rank": rank}
    )

driver.quit()
logging.info(f"LG 모니터 필터 후 {len(items)}개 남음")

# ────────────────────────── 5. DataFrame 및 빈 결과 처리 ──────────────────────────
cols = ["asin", "title", "url", "price", "rank"]
df_today = pd.DataFrame(items, columns=cols)

if df_today.empty:
    logging.info("LG 모니터 없음 → 시트 업데이트 생략")
    sys.exit(0)

df_today = df_today.sort_values("rank").reset_index(drop=True)

kst = pytz.timezone("Asia/Seoul")
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

# ────────────────────────── 6. Google Sheet 기록 (이하 동일) ─────────────────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(
    json.loads(base64.b64decode(os.environ["GCP_SA_BASE64"]).decode()),
    scopes=SCOPES,
)
gc = gspread.authorize(creds)
SHEET_ID = os.environ["SHEET_ID"]
sh = gc.open_by_key(SHEET_ID)

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

for col in ["price", "price_prev", "rank_prev"]:
    df_today[col] = pd.to_numeric(df_today[col], errors="coerce")

df_today["rank_delta_num"] = df_today["rank_prev"] - df_today["rank"]
df_today["price_delta_num"] = df_today["price"] - df_today["price_prev"]

def fmt(val, is_price=False):
    if pd.isna(val) or val == 0:
        return "-"
    arrow = "▲" if val > 0 else "▼"
    return f"{arrow}{abs(val):.2f}" if is_price else f"{arrow}{abs(int(val))}"

df_today["rank_delta"] = df_today["rank_delta_num"].apply(fmt)
df_today["price_delta"] = df_today["price_delta_num"].apply(lambda x: fmt(x, True))

cols_out = [
    "asin",
    "title",
    "rank",
    "price",
    "url",
    "date",
    "rank_delta",
    "price_delta",
]
df_today = df_today[cols_out].fillna("")

# 6-B. 시트 쓰기
if not ws_hist.get_all_values():
    ws_hist.append_row(cols_out, value_input_option="USER_ENTERED")
ws_hist.append_rows(df_today.values.tolist(), value_input_option="USER_ENTERED")

ws_today.clear()
ws_today.update([cols_out] + df_today.values.tolist(), value_input_option="USER_ENTERED")

# 6-C. ▲/▼ 서식
RED = Color(1, 0, 0)
BLUE = Color(0, 0, 1)
delta_cols = {"rank_delta": "G", "price_delta": "H"}
fmt_ranges = []
for i, row in df_today.iterrows():
    r = i + 2
    for col_name, col_letter in delta_cols.items():
        val = row[col_name]
        if isinstance(val, str) and val.startswith("▲"):
            fmt_ranges.append(
                (f"{col_letter}{r}", CellFormat(textFormat=TextFormat(bold=True, foregroundColor=RED)))
            )
        elif isinstance(val, str) and val.startswith("▼"):
            fmt_ranges.append(
                (f"{col_letter}{r}", CellFormat(textFormat=TextFormat(bold=True, foregroundColor=BLUE)))
            )
if fmt_ranges:
    format_cell_ranges(ws_today, fmt_ranges)

logging.info("Google Sheet 업데이트 완료 — LG 모니터 %d개", len(df_today))
print("✓ Google Sheet 업데이트 완료 — LG 모니터", len(df_today))
