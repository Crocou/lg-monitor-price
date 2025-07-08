import os, sys, re, json, datetime, time, logging
import pandas as pd, gspread, pytz
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from gspread_formatting import format_cell_ranges, CellFormat, TextFormat, Color

# ─── 설정 상수 ──────────────────────────────────────────────────────
BASE_URL  = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
CARD_SEL  = "li.zg-no-numbers"

# ─── 로깅 ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("crawl_cards.log", encoding="utf-8"),
              logging.StreamHandler(sys.stdout)],
)
logging.info("🔍 LG 모니터 크롤러 시작")

# ─── Selenium 드라이버 생성 ─────────────────────────────────────────
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

# ─── 카드 파싱 함수 ─────────────────────────────────────────────────
def fetch_cards_and_parse(page: int, driver):
    parsed_items = []

    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
    logging.info("▶️  요청 URL (page %d): %s", page, url)
    driver.get(url)

    # 최소 1장 대기
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, CARD_SEL))
        )
    except TimeoutException:
        logging.error("⛔ page %d: 카드 0개 - 타임아웃", page)
        return []

    # 스크롤 로딩
    start = time.time()
    last, SCROLL_PAUSE, MAX_WAIT = 0, 3, 60
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)
        now = len(driver.find_elements(By.CSS_SELECTOR, CARD_SEL))
        if page == 1 and now < 50 and time.time() - start < MAX_WAIT:
            continue
        if now == last or time.time() - start >= MAX_WAIT:
            break
        last = now

    cards = driver.find_elements(By.CSS_SELECTOR, CARD_SEL)
    logging.info("✅ page %d 카드 %d개", page, len(cards))

    for idx, card in enumerate(cards, 1):
        try:
            rank = int(re.sub(r"\D", "", card.find_element(
                By.XPATH, './/span[contains(@class,"zg-bdg-text")]').text))
        except (NoSuchElementException, ValueError, StaleElementReferenceException):
            continue

        try:
            title = card.find_element(
                By.XPATH, './/div[contains(@class,"_cDEzb_p13n-sc-css-line-clamp-2_EWgCb")]'
            ).text.strip()
        except NoSuchElementException:
            title = card.find_element(By.XPATH, './/img[@alt]').get_attribute("alt").strip()

        lg_match = bool(re.search(r"\bLG\b", title.replace("\u00a0", " "), re.I))

        # 가격 추출 (스트립 문자열 그대로)
        price_raw = card.find_element(
            By.CSS_SELECTOR, 'span._cDEzb_p13n-sc-price_3mJ9Z'
        ).text.strip()
        if not price_raw:
            try:
                offer_txt = card.find_element(
                    By.CSS_SELECTOR, 'span.a-color-secondary'
                ).text.strip()
                m = re.search(r'€[\d\.,]+', offer_txt)
                if m:
                    price_raw = m.group(0)
            except NoSuchElementException:
                pass

        # 링크/ASIN
        try:
            href = card.find_element(By.XPATH, './/a[contains(@href,"/dp/")]').get_attribute("href")
            link = href.split("?",1)[0]
            asin = re.search(r"/dp/([A-Z0-9]{10})", link).group(1)
        except Exception:
            continue

        if lg_match:
            parsed_items.append({
                "asin": asin,
                "title": title,
                "url": link,
                "price": price_raw,
                "rank": rank,
            })

    return parsed_items

# ───  크롤러 실행 ─────────────────────────────────────────────────
driver = get_driver()
try:
    # --- 1) 로그인 페이지 이동 ---
    driver.get("https://www.amazon.de/ap/signin")
    wait = WebDriverWait(driver, 20)

    # --- 2) 아이디 입력 & 다음 ---
    amz_user = os.environ["AMZ_USER"]
    amz_pass = os.environ["AMZ_PASS"]
    wait.until(EC.presence_of_element_located((By.ID, "ap_email"))).send_keys(amz_user)
    wait.until(EC.element_to_be_clickable((By.ID, "continue"))).click()

    # --- 3) 비밀번호 입력 & 로그인 ---
    wait.until(EC.presence_of_element_located((By.ID, "ap_password"))).send_keys(amz_pass)
    wait.until(EC.element_to_be_clickable((By.ID, "signInSubmit"))).click()
    logging.info("🔐 로그인 완료 (%s)", amz_user)

    # --- 4) 베스트셀러 페이지로 이동 후 크롤링 ---
    items = []
    for pg in (1, 2):
        items += fetch_cards_and_parse(pg, driver)

finally:
    driver.quit()

logging.info("LG 모니터 필터 후 %d개 남음", len(items))
# ─── 4. DataFrame & Google Sheet (가격은 문자열 그대로) ─────────────
cols = ["asin", "title", "url", "price", "rank"]
df_today = pd.DataFrame(items, columns=cols)

if df_today.empty:
    logging.info("LG 모니터 없음 → 시트 업데이트 생략")
    sys.exit(0)

df_today = df_today.sort_values("rank").reset_index(drop=True)
kst = pytz.timezone("Asia/Seoul")
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

# price_delta 등 숫자 계산은 생략하거나 필요하면 별도 파싱 후 진행

# ─── 5. Google Sheet 기록 (기존 로직과 동일, price 컬럼은 문자열) ─────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(
    json.loads(base64.b64decode(os.environ["GCP_SA_BASE64"]).decode()),
    scopes=SCOPES,
)
gc = gspread.authorize(creds)
SHEET_ID = os.environ["SHEET_ID"]
sh = gc.open_by_key(SHEET_ID)

ws_hist = sh.worksheet("History") if "History" in [w.title for w in sh.worksheets()] else sh.add_worksheet("History", rows=2000, cols=20)
ws_today = sh.worksheet("Today") if "Today" in [w.title for w in sh.worksheets()] else sh.add_worksheet("Today", rows=100, cols=20)

if not ws_hist.get_all_values():
    ws_hist.append_row(cols + ["date"], value_input_option="USER_ENTERED")
ws_hist.append_rows(df_today[cols + ["date"]].values.tolist(), value_input_option="USER_ENTERED")

ws_today.clear()
ws_today.update([cols + ["date"]] + df_today[cols + ["date"]].values.tolist(), value_input_option="USER_ENTERED")

logging.info("Google Sheet 업데이트 완료 — LG 모니터 %d개", len(df_today))
print("✓ Google Sheet 업데이트 완료 — LG 모니터", len(df_today))
