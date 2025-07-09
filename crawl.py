# crawl_scroll_login.py
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위
- LG 모니터 필터, 가격·순위·변동 기록 (스크롤 포함)
- ★ 로그인 적용 (우편번호 설정 제거)
- 동적 클래스 대신 DOM 구조·텍스트 기반 안정적 셀렉터 적용
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
    opts = webdriver.ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1280,4000")
    opts.add_argument("--lang=de-DE")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0 Safari/537.36"
    )
    return webdriver.Chrome(options=opts)

# ────────────────────────── 2. 상수 정의 ──────────────────────────
BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
CARDS_XPATH = "//div[contains(@class,'a-cardui') and contains(@class,'_cDEzb_card')]//ol/li"

def fetch_cards_and_parse(page: int, driver):
    parsed_items = []
    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
    logging.info(f"▶️ 요청 URL (page {page}): {url}")
    driver.get(url)

    # 통화·언어 쿠키 세팅
    driver.add_cookie({"name": "lc-main",    "value": "de_DE"})
    driver.add_cookie({"name": "i18n-prefs", "value": "EUR"})
    driver.refresh()

    # 최소 하나라도 로드될 때까지 대기
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, CARDS_XPATH))
        )
    except TimeoutException:
        logging.error(f"⛔ page {page}: 카드 없음 — 타임아웃")
        return []

    # 스크롤하며 추가 로드
    start, last_count = time.time(), 0
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(5)
        cards = driver.find_elements(By.XPATH, CARDS_XPATH)
        curr = len(cards)
        if (page == 1 and curr < 50 and time.time() - start < 60) or (curr != last_count and time.time() - start < 60):
            last_count = curr
            continue
        break

    logging.info(f"✅ page {page} 카드 수집 완료: {len(cards)}개")

    for idx, card in enumerate(cards, start=1):
        # 랭크 추출
        try:
            rank_el = card.find_element(By.XPATH, './/span[contains(text(), "#")]')
            rank = int(re.sub(r"\D", "", rank_el.text.strip()))
        except Exception:
            logging.warning(f"[{idx}] 랭크 추출 실패 → 건너뜀")
            continue

        # 제목 추출
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
        title = title.replace("\u00a0", " ").replace("\u202f", " ")
        lg_match = bool(re.search(r"\bLG\b", title, re.I))

        # 가격 추출
        try:
            price_raw = ""
            selectors = [
                ('xpath', './/span[@class="a-offscreen"]'),
                ('css',   'span.a-price > span.a-offscreen'),
                ('xpath', './/*[contains(@class, "price")]'),
                ('css',   'span.p13n-sc-price'),
            ]
            for method, sel in selectors:
                try:
                    txt = (card.find_element(By.XPATH, sel).text if method=='xpath'
                           else card.find_element(By.CSS_SELECTOR, sel).text).strip()
                    if '€' in txt:
                        price_raw = txt
                        break
                except NoSuchElementException:
                    continue
            if not price_raw:
                raise NoSuchElementException("유효한 가격 요소 없음")
        except Exception:
            logging.warning(f"[{idx}] 가격 추출 실패 → 빈 문자열로 대체")
            price_raw = ""

        # 링크·ASIN 추출
        try:
            link_el = card.find_element(By.XPATH, './/a[contains(@href,"/dp/")]')
            href = link_el.get_attribute("href").split("?",1)[0]
            asin = re.search(r"/dp/([A-Z0-9]{10})", href).group(1)
        except Exception:
            logging.warning(f"[{idx}] 링크/ASIN 추출 실패 → 건너뜀")
            continue

        info = {"rank": rank, "title": title, "price_text": price_raw, "asin": asin, "url": href, "lg_match": lg_match}
        logging.info(f"CARD_DATA {json.dumps(info, ensure_ascii=False)}")

        if lg_match:
            parsed_items.append({"asin": asin, "title": title, "url": href, "price": price_raw, "rank": rank})

    return parsed_items

# ────────────────────────── 3. 크롤링 및 로그인 적용 ──────────────────────────
driver = get_driver()
wait = WebDriverWait(driver, 20)
try:
    # 1) Amazon 로그인
    driver.get("https://www.amazon.de/-/en/ap/signin?openid.pape.max_auth_age=0&openid.return_to=https%3A%2F%2Fwww.amazon.de%2Fref%3Dnav_signin&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.assoc_handle=deflex&openid.mode=checkid_setup&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0")
    amz_user = os.environ["AMZ_USER"]
    amz_pass = os.environ["AMZ_PASS"]
    wait.until(EC.presence_of_element_located((By.ID, "ap_email"))).send_keys(amz_user)
    wait.until(EC.element_to_be_clickable((By.ID, "continue"))).click()
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

# ────────────────────────── 4. DataFrame 생성 및 Google Sheet 기록 ──────────────────────────
cols = ["asin","title","rank","price","url","date","rank_delta","price_delta"]
df = pd.DataFrame(items)
if df.empty:
    logging.info("LG 모니터 없음 → 업데이트 생략")
    sys.exit(0)

df = df.sort_values("rank").reset_index(drop=True)
kst = pytz.timezone("Asia/Seoul")
df["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

# Google Sheet 접속
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(
    json.loads(base64.b64decode(os.environ["GCP_SA_BASE64"]).decode()),
    scopes=SCOPES
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(os.environ["SHEET_ID"])

# History, Today 시트 준비
ws_hist = sh.worksheet("History") if "History" in [w.title for w in sh.worksheets()] else sh.add_worksheet("History", 2000, 20)
ws_today = sh.worksheet("Today")   if "Today"   in [w.title for w in sh.worksheets()] else sh.add_worksheet("Today",   100, 20)

# 이전 기록 불러오기
try:
    prev = pd.DataFrame(ws_hist.get_all_records()).dropna()
except:
    prev = pd.DataFrame()

if not prev.empty and {"asin","rank","price","date"} <= set(prev.columns):
    last = prev.sort_values("date").groupby("asin", as_index=False).last()[["asin","rank","price"]]
    last.columns = ["asin","rank_prev","price_prev"]
    df = df.merge(last, on="asin", how="left")
else:
    df["rank_prev"] = None
    df["price_prev"] = None

# 변동 계산
import pandas as pd  # ensure pandas imported

df["rank_delta"]  = df["rank_prev"].combine(df["rank"], lambda prev,curr: "-" if pd.isna(prev) else f"{'▲' if prev>curr else '▼'}{abs(int(prev-curr))}")
df["price_delta"] = "-"

out_cols = ["asin","title","rank","price","url","date","rank_delta","price_delta"]
df_out   = df[out_cols].fillna("")

# 시트에 기록
if not ws_hist.get_all_values():
    ws_hist.append_row(out_cols, value_input_option="USER_ENTERED")
ws_hist.append_rows(df_out.values.tolist(), value_input_option="USER_ENTERED")
ws_today.clear()
ws_today.update([out_cols] + df_out.values.tolist(), value_input_option="USER_ENTERED")

# 서식 적용
RED, BLUE = Color(1,0,0), Color(0,0,1)
fmt_ranges = []
for i, row in df_out.iterrows():
    r = i + 2
    for col, letter in [("rank_delta","G"),("price_delta","H")]:
        v = row[col]
        if v.startswith("▲"):
            fmt_ranges.append((f"{letter}{r}", CellFormat(textFormat=TextFormat(bold=True, foregroundColor=RED))))
        elif v.startswith("▼"):
            fmt_ranges.append((f"{letter}{r}", CellFormat(textFormat=TextFormat(bold=True, foregroundColor=BLUE))))
if fmt_ranges:
    format_cell_ranges(ws_today, fmt_ranges)

logging.info("Google Sheet 업데이트 완료 — LG 모니터 %d개", len(df_out))
