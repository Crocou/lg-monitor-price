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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


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
    driver.add_cookie({"name": "deliveryZip", "value": zip_code})

BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"  # pg=1|2
CARD_SEL = (
    "li.zg-no-numbers"
)

# ────────────────────────── 2. 페이지에서 카드 가져오기 ──────────────────────────
BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
CARD_SEL = "li.zg-no-numbers"


def money_to_float(txt: str):
    """'€196,79' 또는 '€196.79' → 196.79 (float)"""
    if not txt:
        return None
    # NBSP·좁은공백 제거
    txt = txt.replace("\u00a0", "").replace("\u202f", "")
    # 통화·문자 제거
    txt_clean = re.sub(r"[^\d,\.]", "", txt)

    # 소수 구분자 감지
    if "," in txt_clean and "." in txt_clean:
        # 마지막 구분자를 소수점으로 간주
        if txt_clean.rfind(",") > txt_clean.rfind("."):
            txt_clean = txt_clean.replace(".", "").replace(",", ".")
        else:
            txt_clean = txt_clean.replace(",", "")
    elif "," in txt_clean and "." not in txt_clean:
        txt_clean = txt_clean.replace(",", ".")
    else:
        txt_clean = txt_clean  # 이미 '.'만 있거나 구분자 없음

    try:
        return float(txt_clean)
    except ValueError:
        logging.warning(f"가격 변환 실패: {txt}")
        return None

def fetch_cards_and_parse(page: int, driver):
    parsed_items = []
    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
    logging.info("▶️  요청 URL (page %d): %s", page, url)
    driver.get(url)

    # ── 배송지·통화 쿠키 ───────────────────────────────────────────
    driver.add_cookie({"name": "lc-main", "value": "de_DE"})
    driver.add_cookie({"name": "i18n-prefs", "value": "EUR"})
    # ① 배송지 쿠키가 없으면 즉시 주입
    if not driver.get_cookie("deliveryZip"):
        driver.add_cookie({"name": "deliveryZip", "value": "65760"})
    driver.refresh()
    logging.info("쿠키 확인 → deliveryZip=%s", driver.get_cookie("deliveryZip"))

    # ─── ★ 최소 한 장이라도 뜰 때까지 대기 (최대 20초) ───
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, CARD_SEL))
        )
    except TimeoutException:
        logging.error(f"⛔ page {page}: 카드가 한 장도 안 뜸 — 타임아웃")
        return []                # 바로 빈 리스트 반환해 다음 페이지 시도

    # ─── 스크롤하면서 추가 카드 로딩 ───
    SCROLL_PAUSE = 10
    MAX_WAIT = 60                # 스크롤 최대 대기(초) — 필요에 맞게 조정
    start = time.time()
    last = 0
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)

        cards = driver.find_elements(By.CSS_SELECTOR, CARD_SEL)
        now = len(cards)

        # page 1이라면 50개 꽉 찰 때까지 시도 (필요 없으면 조건 삭제)
        if page == 1 and now < 50 and time.time() - start < MAX_WAIT:
            continue

        if now == last or time.time() - start >= MAX_WAIT:
            break
        last = now

    logging.info(f"✅ page {page} 카드 수집 완료: {len(cards)}개")


    for idx, card in enumerate(cards, start=1):
        # ───── 랭크 ─────
        try:
            rank_el = card.find_element(By.XPATH, './/span[contains(@class,"zg-bdg-text")]')
            rank_text = rank_el.text.strip()
            rank = int(re.sub(r"\D", "", rank_text))  # "#1" → "1"
        except (NoSuchElementException, ValueError, StaleElementReferenceException):
            logging.warning(f"[{idx}] 랭크 추출 실패 → 건너뜀")
            continue

        # ───── 제목 ─────
        try:
            try:
                title = card.find_element(By.XPATH, './/div[contains(@class,"_cDEzb_p13n-sc-css-line-clamp-2_EWgCb")]').text.strip()
            except NoSuchElementException:
                title = card.find_element(By.XPATH, './/img[@alt]').get_attribute("alt").strip()
        except Exception:
            title = ""

        # NBSP 대체 후 LG 여부 체크
        title_norm = title.replace("\u00a0", " ").replace("\u202f", " ")
        lg_match = bool(re.search(r"\bLG\b", title_norm, re.I))

        # ───── 가격 ─────
        price_raw = ""
        # ② 1순위: 카드 안의 ‘오늘 가격’
        try:
            price_raw = card.find_element(
                By.CSS_SELECTOR, 'span._cDEzb_p13n-sc-price_3mJ9Z'
            ).text.strip()
        except NoSuchElementException:
            pass

        # ③ 2순위: ‘3 offers from €123’ 형태
        if not price_raw:
            try:
                offer_txt = card.find_element(
                    By.CSS_SELECTOR, 'span.a-color-secondary'
                ).text.strip()          # 예: "3 offers from €123.45"
                m = re.search(r'€[\d\.,]+', offer_txt)
                if m:
                    price_raw = m.group(0)   # "€123.45" 추출
            except NoSuchElementException:
                pass

        price_val = money_to_float(price_raw)

        # ───── 링크/ASIN ─────
        try:
            a = card.find_element(By.XPATH, './/a[contains(@href,"/dp/")]')
            href = a.get_attribute("href")
            link = href.split("?", 1)[0] if href.startswith("http") else "https://www.amazon.de" + href.split("?", 1)[0]
            asin = re.search(r"/dp/([A-Z0-9]{10})", link).group(1)
        except Exception:
            logging.warning(f"[{idx}] 링크/ASIN 추출 실패 → 건너뜀")
            continue

        # ───── 로깅 및 결과 ─────
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

# ────────────────────────── 5. Google Sheet 기록 (이하 동일) ─────────────────────
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
