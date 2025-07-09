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
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, TimeoutException
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
    return webdriver.Chrome(options=opt)

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

# ────────────────────────── 2. 상수 정의 ──────────────────────────
BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
# 변경된 카드 컨테이너 셀렉터
CARD_SEL = "div.zg-grid-general-faceout"

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

def fetch_cards_and_parse(page: int, driver):
    parsed_items = []
    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
    logging.info(f"▶️  요청 URL (page {page}): {url}")
    driver.get(url)

    # 배송지·통화 쿠키 세팅 후 새로고침
    driver.add_cookie({"name": "lc-main", "value": "de_DE"})
    driver.add_cookie({"name": "i18n-prefs", "value": "EUR"})
    driver.refresh()

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, CARD_SEL))
        )
    except TimeoutException:
        logging.error(f"⛔ page {page}: 카드가 한 장도 안 뜸 — 타임아웃")
        return []

    # 스크롤하면서 추가 카드 로딩
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

    for idx, card in enumerate(cards, start=1):
        # 랭크
        try:
            rank_el = card.find_element(By.XPATH, './/span[contains(@class,"zg-bdg-text")]')
            rank = int(re.sub(r"\D", "", rank_el.text.strip()))
        except Exception:
            logging.warning(f"[{idx}] 랭크 추출 실패 → 건너뜀")
            continue

        # 제목
        try:
            title = card.find_element(
                By.XPATH,
                './/div[contains(@class,"_cDEzb_p13n-sc-css-line-clamp-2_EWgCb")]'
            ).text.strip()
        except NoSuchElementException:
            try:
                title = card.find_element(By.XPATH, './/img[@alt]').get_attribute("alt").strip()
            except Exception:
                title = ""
        title_norm = title.replace("\u00a0", " ").replace("\u202f", " ")
        lg_match = bool(re.search(r"\bLG\b", title_norm, re.I))

        # 가격(raw)
        try:
            price_raw = card.find_element(
                By.CSS_SELECTOR, 
                "span.a-price > span.a-offscreen"
            ).text.strip()
        except NoSuchElementException:
            price_raw = card.find_element(
                By.CSS_SELECTOR,
                "span.p13n-sc-price"
            ).text.strip()
        price_val = price_raw  # float 변환 없이 raw 문자열 그대로

        # 링크·ASIN
        try:
            a = card.find_element(By.XPATH, './/a[contains(@href,"/dp/")]')
            href = a.get_attribute("href")
            link = href.split("?", 1)[0]
            asin = re.search(r"/dp/([A-Z0-9]{10})", link).group(1)
        except Exception:
            logging.warning(f"[{idx}] 링크/ASIN 추출 실패 → 건너뜀")
            continue

        # 로깅
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

        if lg_match:
            parsed_items.append({
                "asin": asin,
                "title": title,
                "url": link,
                "price": price_val,
                "rank": rank,
            })

    return parsed_items

# ────────────────────────── 3. 수집 및 파싱 ──────────────────────────
driver = get_driver()
driver.get("https://www.amazon.de/")
set_zip(driver, "65760")

items = []
for pg in (1, 2):
    items += fetch_cards_and_parse(pg, driver)

driver.quit()
logging.info(f"LG 모니터 필터 후 {len(items)}개 남음")

# ────────────────────────── 4. DataFrame 및 빈 결과 처리 ──────────────────────────
cols = ["asin", "title", "url", "price", "rank"]
df_today = pd.DataFrame(items, columns=cols)

if df_today.empty:
    logging.info("LG 모니터 없음 → 시트 업데이트 생략")
    sys.exit(0)

df_today = df_today.sort_values("rank").reset_index(drop=True)
kst = pytz.timezone("Asia/Seoul")
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

# ────────────────────────── 5. Google Sheet 기록 ──────────────────────────
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

# rank_prev만 numeric, price는 raw 문자열이므로 변환 제외
df_today["rank_prev"] = pd.to_numeric(df_today["rank_prev"], errors="coerce")

df_today["rank_delta_num"] = df_today["rank_prev"] - df_today["rank"]
df_today["rank_delta"] = df_today["rank_delta_num"].apply(
    lambda x: "-" if pd.isna(x) or x == 0 else ("▲"+str(int(abs(x))) if x > 0 else "▼"+str(int(abs(x))))
)
# price_delta는 raw price만 보여줄 경우 모두 "-"
df_today["price_delta"] = "-"

cols_out = ["asin", "title", "rank", "price", "url", "date", "rank_delta", "price_delta"]
df_today = df_today[cols_out].fillna("")

# ────────────────────────── 6. 시트 쓰기 ──────────────────────────
if not ws_hist.get_all_values():
    ws_hist.append_row(cols_out, value_input_option="USER_ENTERED")
ws_hist.append_rows(df_today.values.tolist(), value_input_option="USER_ENTERED")

ws_today.clear()
ws_today.update([cols_out] + df_today.values.tolist(), value_input_option="USER_ENTERED")

# ────────────────────────── 7. ▲/▼ 서식 ──────────────────────────
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
