# crawl_scroll_zip65760.py
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위
- LG 모니터 필터, 가격·순위·변동 기록 (스크롤 포함)
- ★ 배송지(우편번호) 65760 고정
"""

import sys, os, re, json, base64, datetime, time, logging
import pandas as pd, gspread, pytz
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,   # ★ 추가
    TimeoutException,                 # (기존)
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
CARD_SEL = "li.zg-no-numbers"

# ────────────────────────── 2. 유틸 ──────────────────────────
def money_to_float(txt: str):
    """'€196,79' 또는 '€196.79' → 196.79 (float)"""
    if not txt:
        return None
    txt = txt.replace("\u00a0", "").replace("\u202f", "")
    txt_clean = re.sub(r"[^\d,\.]", "", txt)
    if "," in txt_clean and "." in txt_clean:
        if txt_clean.rfind(",") > txt_clean.rfind("."):
            txt_clean = txt_clean.replace(".", "").replace(",", ".")
        else:
            txt_clean = txt_clean.replace(",", "")
    elif "," in txt_clean and "." not in txt_clean:
        txt_clean = txt_clean.replace(",", ".")
    try:
        return float(txt_clean)
    except ValueError:
        logging.warning(f"가격 변환 실패: {txt}")
        return None

# ────────────────────────── 3. 페이지에서 카드 가져오기 ──────────────────────────
def fetch_cards_and_parse(page: int, driver):
    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
    logging.info(f"▶️  요청 URL (page {page}): {url}")
    driver.get(url)

    driver.add_cookie({"name": "lc-main", "value": "de_DE"})
    driver.add_cookie({"name": "i18n-prefs", "value": "EUR"})
    driver.refresh()

    # ★ 최소 한 장이라도 뜰 때까지 대기
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, CARD_SEL))
        )
    except TimeoutException:
        logging.error(f"⛔ page {page}: 카드가 한 장도 안 뜸 — 타임아웃")
        return []

    # ─── 스크롤하면서 추가 카드 로딩 ───
    SCROLL_PAUSE = 10
    MAX_WAIT = 60
    start = time.time()
    last = 0
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)

        cards = driver.find_elements(By.CSS_SELECTOR, CARD_SEL)
        now = len(cards)

        if page == 1 and now < 50 and time.time() - start < MAX_WAIT:
            continue

        if now == last or time.time() - start >= MAX_WAIT:
            break
        last = now

    logging.info(f"✅ page {page} 카드 수집 완료: {len(cards)}개")

    parsed_items = []            # ★ 리스트를 반드시 먼저 만든다

    for idx, card in enumerate(cards, start=1):
        # 랭크
        try:
            rank_el = card.find_element(By.XPATH, './/span[contains(@class,"zg-bdg-text")]')
            rank_text = rank_el.text.strip()
            rank = int(re.sub(r"\D", "", rank_text))
        except (NoSuchElementException, ValueError, StaleElementReferenceException):
            continue

        # 제목
        try:
            try:
                title = card.find_element(
                    By.XPATH, './/div[contains(@class,"_cDEzb_p13n-sc-css-line-clamp-2_EWgCb")]'
                ).text.strip()
            except NoSuchElementException:
                title = card.find_element(By.XPATH, './/img[@alt]').get_attribute("alt").strip()
        except Exception:
            title = ""

        lg_match = bool(re.search(r"\bLG\b", title.replace("\u00a0", " ").replace("\u202f", " "), re.I))

        # 가격
        try:
            price_raw = card.find_element(By.XPATH, './/span[contains(@class,"p13n-sc-price")]').text.strip()
        except NoSuchElementException:
            price_raw = ""
        price_val = money_to_float(price_raw)

        # 링크/ASIN
        try:
            a = card.find_element(By.XPATH, './/a[contains(@href,"/dp/")]')
            href = a.get_attribute("href")
            link = href.split("?", 1)[0] if href.startswith("http") else "https://www.amazon.de" + href.split("?", 1)[0]
            asin = re.search(r"/dp/([A-Z0-9]{10})", link).group(1)
        except Exception:
            continue

        if lg_match:
            parsed_items.append({          # ← NameError 발생하던 부분
                "asin":  asin,
                "title": title,
                "url":   link,
                "price": price_val,
                "rank":  rank,
            })

    return parsed_items

# ────────────────────────── 4. 수집 및 파싱 ──────────────────────────
driver = get_driver()
driver.get("https://www.amazon.de/")
set_zip(driver, "65760")

items = []
for pg in (1, 2):
    items += fetch_cards_and_parse(pg, driver)

driver.quit()
logging.info(f"LG 모니터 필터 후 {len(items)}개 남음")

# ────────────────────────── 5. DataFrame 및 시트 처리 (원본 그대로) ────────────────
cols = ["asin", "title", "url", "price", "rank"]
df_today = pd.DataFrame(items, columns=cols)

if df_today.empty:
    logging.info("LG 모니터 없음 → 시트 업데이트 생략")
    sys.exit(0)

df_today = df_today.sort_values("rank").reset_index(drop=True)
kst = pytz.timezone("Asia/Seoul")
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(
    json.loads(base64.b64decode(os.environ["GCP_SA_BASE64"]).decode()),
    scopes=SCOPES,
)
gc = gspread.authorize(creds)
SHEET_ID = os.environ["SHEET_ID"]
sh = gc.open_by_key(SHEET_ID)

ws_hist = sh.worksheet("History") if "History" in [w.title for w in sh.worksheets()] else sh.add_worksheet("History", rows=2000, cols=20)
ws_today = sh.worksheet("Today")   if "Today"   in [w.title for w in sh.worksheets()] else sh.add_worksheet("Today", rows=100,  cols=20)

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
    df_today["rank_prev"]  = None
    df_today["price_prev"] = None

for col in ["price", "price_prev", "rank_prev"]:
    df_today[col] = pd.to_numeric(df_today[col], errors="coerce")

df_today["rank_delta_num"]  = df_today["rank_prev"]  - df_today["rank"]
df_today["price_delta_num"] = df_today["price"]      - df_today["price_prev"]

fmt = lambda v, p=False: "-" if pd.isna(v) or v == 0 else ("▲" if v > 0 else "▼") + (f"{abs(v):.2f}" if p else f"{abs(int(v))}")
df_today["rank_delta"]  = df_today["rank_delta_num"].apply(fmt)
df_today["price_delta"] = df_today["price_delta_num"].apply(lambda x: fmt(x, True))

cols_out = ["asin", "title", "rank", "price", "url", "date", "rank_delta", "price_delta"]
df_today = df_today[cols_out].fillna("")

if not ws_hist.get_all_values():
    ws_hist.append_row(cols_out, value_input_option="USER_ENTERED")
ws_hist.append_rows(df_today.values.tolist(), value_input_option="USER_ENTERED")

ws_today.clear()
ws_today.update([cols_out] + df_today.values.tolist(), value_input_option="USER_ENTERED")

RED, BLUE = Color(1, 0, 0), Color(0, 0, 1)
delta_cols = {"rank_delta": "G", "price_delta": "H"}
fmt_ranges = []
for i, row in df_today.iterrows():
    r = i + 2
    for col_name, col_letter in delta_cols.items():
        val = row[col_name]
        if isinstance(val, str) and val.startswith("▲"):
            fmt_ranges.append((f"{col_letter}{r}", CellFormat(textFormat=TextFormat(bold=True, foregroundColor=RED))))
        elif isinstance(val, str) and val.startswith("▼"):
            fmt_ranges.append((f"{col_letter}{r}", CellFormat(textFormat=TextFormat(bold=True, foregroundColor=BLUE))))
if fmt_ranges:
    format_cell_ranges(ws_today, fmt_ranges)

logging.info("Google Sheet 업데이트 완료 — LG 모니터 %d개", len(df_today))
print("✓ Google Sheet 업데이트 완료 — LG 모니터", len(df_today))
