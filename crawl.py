import sys, os, re, json, base64, datetime, time, logging, pytz
import pandas as pd, gspread
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

BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
CARD_SEL = "li.zg-no-numbers"

# ────────────────────────── 2. 카드 크롤링 & 파싱 ─────────────────────────
def fetch_cards_and_parse(page: int, driver):
    parsed_items = []
    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
    logging.info(f"▶️  요청 URL (page {page}): {url}")
    driver.get(url)

    # ─── 스크롤하면서 추가 카드 로딩 (최대 대기 60초) ───
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
            rank = int(re.sub(r"\D", "", card.find_element(
                By.XPATH, './/span[contains(@class,"zg-bdg-text")]'
            ).text.strip()))
        except (NoSuchElementException, ValueError, StaleElementReferenceException):
            logging.warning(f"[{idx}] 랭크 추출 실패 → 건너뜀")
            continue

        # 제목
        try:
            title = card.find_element(
                By.XPATH, './/div[contains(@class,"_cDEzb_p13n-sc-css-line-clamp-2_EWgCb")]'
            ).text.strip()
        except NoSuchElementException:
            title = card.find_element(By.XPATH, './/img[@alt]').get_attribute("alt").strip()

        # LG 필터
        title_norm = title.replace("\u00a0", " ").replace("\u202f", " ")
        if not re.search(r"\bLG\b", title_norm, re.I):
            continue

        # 가격 (문자열 그대로)
        try:
            price = card.find_element(
                By.CSS_SELECTOR, 'span._cDEzb_p13n-sc-price_3mJ9Z'
            ).text.strip()
        except NoSuchElementException:
            price = ""

        # 링크 & ASIN
        try:
            href = card.find_element(
                By.XPATH, './/a[contains(@href,"/dp/")]'
            ).get_attribute("href")
            link = href.split("?", 1)[0]
            asin = re.search(r"/dp/([A-Z0-9]{10})", link).group(1)
        except Exception:
            logging.warning(f"[{idx}] 링크/ASIN 추출 실패 → 건너뜀")
            continue

        parsed_items.append({
            "asin": asin,
            "title": title,
            "url": link,
            "price": price,
            "rank": rank,
        })

    return parsed_items

# ────────────────────────── 3. 크롤러 실행 & 로그인 ─────────────────────────
driver = get_driver()
try:
    # --- 1) 로그인 페이지 이동 ---
    driver.get("https://www.amazon.de/-/en/ap/signin?openid.pape.max_auth_age=0&openid.return_to=https%3A%2F%2Fwww.amazon.de%2Fref%3Dnav_signin&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.assoc_handle=deflex&openid.mode=checkid_setup&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0")
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

    # 2) 페이지별 크롤링
    items = []
    for pg in (1, 2):
        try:
            items += fetch_cards_and_parse(pg, driver)
        except TimeoutException:
            logging.error(f"⛔ page {pg}: 카드 로딩 타임아웃")
finally:
    driver.quit()

logging.info(f"LG 모니터 필터 후 {len(items)}개 남음")

# ────────────────────────── 4. DataFrame 생성 및 후처리 ──────────────────────────
cols = ["asin", "title", "url", "price", "rank"]
df_today = pd.DataFrame(items, columns=cols)

if df_today.empty:
    logging.info("LG 모니터 없음 → 시트 업데이트 생략")
    sys.exit(0)

df_today = df_today.sort_values("rank").reset_index(drop=True)
kst = pytz.timezone("Asia/Seoul")
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

# 이전 데이터 병합 및 델타 계산
creds = Credentials.from_service_account_info(
    json.loads(base64.b64decode(os.environ["GCP_SA_BASE64"]).decode()),
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(os.environ["SHEET_ID"])

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

# 히스토리 시트 업데이트
if not ws_hist.get_all_values():
    ws_hist.append_row(cols + ["date"], value_input_option="USER_ENTERED")
prev = pd.DataFrame(ws_hist.get_all_records()).dropna()
if not prev.empty and {"asin","rank","price","date"} <= set(prev.columns):
    latest = (
        prev.sort_values("date")
            .groupby("asin", as_index=False)
            .last()[["asin","rank","price"]]
            .rename(columns={"rank":"rank_prev","price":"price_prev"})
    )
    df_today = df_today.merge(latest, on="asin", how="left")
else:
    df_today["rank_prev"] = None
    df_today["price_prev"] = None

# 수치 변환 및 델타
for col in ["price","price_prev","rank_prev"]:
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

# Today 시트 및 포맷 업데이트
cols_out = ["asin","title","rank","price","url","date","rank_delta","price_delta"]
if not ws_hist.get_all_values():  # 헤더 삽입
    ws_hist.append_row(cols_out, value_input_option="USER_ENTERED")
ws_hist.append_rows(df_today[cols_out].values.tolist(), value_input_option="USER_ENTERED")

ws_today.clear()
ws_today.update([cols_out] + df_today[cols_out].values.tolist(), value_input_option="USER_ENTERED")

# 델타 컬럼 서식
RED = Color(1,0,0)
BLUE = Color(0,0,1)
fmt_ranges = []
for i, row in df_today.iterrows():
    r = i + 2
    for col_name, col_letter in {"rank_delta":"G","price_delta":"H"}.items():
        val = row[col_name]
        if isinstance(val,str) and val.startswith("▲"):
            fmt_ranges.append((f"{col_letter}{r}", CellFormat(textFormat=TextFormat(bold=True, foregroundColor=RED))))
        elif isinstance(val,str) and val.startswith("▼"):
            fmt_ranges.append((f"{col_letter}{r}", CellFormat(textFormat=TextFormat(bold=True, foregroundColor=BLUE))))
if fmt_ranges:
    format_cell_ranges(ws_today, fmt_ranges)

logging.info(f"Google Sheet 업데이트 완료 — LG 모니터 {len(df_today)}개")
print(f"✓ Google Sheet 업데이트 완료 — LG 모니터 {len(df_today)}개")
