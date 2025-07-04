#!/usr/bin/env python3
"""
Amazon.de Monitors Bestseller Scraper (TRACE ‑ Full Log)
=======================================================
• 페이지(pg=1,2 …)마다
  ① 기본 HTML (1~30위)
  ② 스크롤 ajax (?pg=N&ajax=1, 31~50위)
  → 두 응답 병합, 중복 제거 후 최대 50위 확보
• 100개 수집 시 조기 종료
• **모든 컨테이너·모든 아이템** 제목/가격/순위 전체 로그
• Session+Retry, 1 s polite delay
• LG 모니터만 필터 → Google Sheet(History append, Today replace)

필수 ENV
---------
GCP_SA_BASE64   – base64‑encoded Service Account JSON
SHEET_ID        – target Google Sheet ID

선택 ENV
--------
LOG_LEVEL       – DEBUG/INFO (default INFO)
SCRAPER_UA      – UA override
SCRAPER_TZ      – IANA TZ (default Asia/Seoul)
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import logging
import os
import re
import textwrap
import time
from typing import List, Tuple

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
import gspread
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ───────── Logging ─────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger(__name__)
BAR = "—" * 90

# ───────── Constants ─────────
BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
HEADERS = {
    "User-Agent": os.getenv(
        "SCRAPER_UA",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/126.0",
    ),
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
}
COOKIES = {"lc-main": "de_DE", "i18n-prefs": "EUR"}
TIMEZONE = pytz.timezone(os.getenv("SCRAPER_TZ", "Asia/Seoul"))

CSS_SELECTORS = [
    "div.zg-grid-general-faceout",
    "div.p13n-sc-uncoverable-faceout",
    "div.a-section.a-spacing-none.p13n-asin",
    "ol#zg-ordered-list > li",
]

# ───────── Helpers ─────────

def money(text: str) -> float | None:
    if not text:
        return None
    num = re.sub(r"[^0-9,\.]", "", text).replace(".", "").replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return None


def parse_cards(html: str) -> List[BeautifulSoup]:
    soup = BeautifulSoup(html, "lxml")
    for sel in CSS_SELECTORS:
        found = soup.select(sel)
        if found:
            return found
    return []


# ───────── Network ─────────

def init_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(HEADERS)
    return s


def fetch(url: str, session: requests.Session) -> str:
    logger.debug("GET %s", url)
    r = session.get(url, cookies=COOKIES, timeout=30)
    logger.debug("Status %s, length %d", r.status_code, len(r.text))
    if r.status_code != 200:
        logger.warning("Non‑200 response: %s", url)
    return r.text


# ───────── Scraping ─────────

def fetch_page_cards(page: int, session: requests.Session) -> List[Tuple[BeautifulSoup, int]]:
    """Return list of (card, page_idx) tuples for the given page."""
    urls = [BASE_URL if page == 1 else f"{BASE_URL}?pg={page}", f"{BASE_URL}?pg={page}&ajax=1"]
    cards: List[Tuple[BeautifulSoup, int]] = []
    for label, url in zip(["MAIN", "AJAX"], urls):
        print(f"\n{BAR}\n[REQUEST] {label} {url}")
        html = fetch(url, session)
        print(
            f"[RESPONSE] len={len(html)} | sample="
            f"{textwrap.shorten(html.replace('\n', ' '), width=120, placeholder='…')}\n{BAR}"
        )
        part_cards = parse_cards(html)
        print(f"[PARSE] {label} containers={len(part_cards)}")
        for i, c in enumerate(part_cards, 1):
            title = c.get_text(' ', strip=True)[:90]
            price_el = c.select_one('span.a-offscreen')
            price_txt = price_el.get_text(strip=True) if price_el else '—'
            print(f"   {i:>2}. {title} | {price_txt}")
        cards.extend([(c, page) for c in part_cards])
    return cards


def scrape() -> pd.DataFrame:
    session = init_session()
    all_cards: List[Tuple[BeautifulSoup, int]] = []
    page = 1
    while len(all_cards) < 100:
        page_cards = fetch_page_cards(page, session)
        if not page_cards:
            logger.warning("No cards on page %d – stopping", page)
            break
        # deduplicate
        seen: set[str] = set()
        for card, pg in page_cards:
            key = card.get("data-asin") or card.encode()[:100]
            if key in seen:
                continue
            seen.add(key)
            all_cards.append((card, pg))
        logger.info("Accumulated cards: %d", len(all_cards))
        if len(all_cards) >= 100:
            break
        page += 1
        time.sleep(1)

    print(f"\n[TRACE] total containers={len(all_cards)}")

    # ---------------- Parse items ----------------
    items = []
    for idx, (card, pg_idx) in enumerate(all_cards, 1):
        badge = card.select_one('.zg-badge-text')
        rank_pg = int(badge.get_text(strip=True).lstrip('#')) if badge else ((idx-1) % 50 + 1)
        abs_rank = (pg_idx - 1) * 50 + rank_pg
        a = card.select_one("a.a-link-normal[href*='/dp/']")
        if not a:
            continue
        url = "https://www.amazon.de" + a['href'].split('?', 1)[0]
        asin_m = re.search(r"/dp/([A-Z0-9]{10})", url)
        asin = asin_m.group(1) if asin_m else None
        title_tag = card.select_one('[title]')
        title = title_tag['title'].strip() if title_tag else a.get_text(' ', strip=True)
        price_tag = card.select_one('span.a-offscreen') or card.select_one('span.p13n-sc-price')
        price = money(price_tag.get_text(strip=True)) if price_tag else None
        items.append({
            'asin': asin,
            'title': title,
            'rank': abs_rank,
            'price': price,
            'url': url,
        })

    print(f"\n[TRACE] items scraped={len(items)} (full list)")
    for itm in sorted(items, key=lambda x: x['rank']):
        print(f" #{itm['rank']:>3} | {itm['asin'] or '—'} | {itm['title'][:90]} | {itm['price']}")

    df = pd.DataFrame(items).drop_duplicates('asin').sort_values('rank')
    df_lg = df[df['title'].str.upper().str.contains('LG')].copy()
    if df_lg.empty:
        raise RuntimeError('LG 모니터 없음')

    print("\n[LG LIST]")
    for r in df_lg.itertuples():
        print(f" #{r.rank:>3} | {r.asin} | {r.title[:90]} | {r.price}")

    df_lg['date'] = dt.datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
    return df_lg


# ───────── Google Sheets Upload ─────────

def upload_to_sheets(df: pd.DataFrame) -> None:
    for var in ('GCP_SA_BASE64', 'SHEET_ID'):
        if var not in os.environ:
            raise EnvironmentError(f"Missing env var {var}")

    creds_info = json.loads(base64.b64decode(os.environ['GCP_SA_BASE64']).decode())
    creds = Credentials.from_service_account_info(creds_info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    sh = gspread.authorize(creds).open_by_key(os.environ['SHEET_ID'])
    sheets = {w.title: w for w in sh.worksheets()}
    ws_hist = sheets.get('History') or sh.add_worksheet('History', 2000, 20)
    ws_today = sheets.get('Today') or sh.add_worksheet('Today', 100, 20)

    cols = ['asin', 'title', 'rank', 'price', 'url', 'date']
    df = df[cols].fillna('')

    if not ws_hist.get_all_values():
        ws_hist.append_row(cols, value_input_option='RAW')
    ws_hist.append_rows(df.values.tolist(), value_input_option='RAW')

    ws_today.clear()
    ws_today.update([cols] + df.values.tolist(), value_input_option='RAW')
    logger.info('Sheets updated: %d LG rows', len(df))


# ───────── Main ─────────

def main():
    logger.info('BEGIN scrape')
    df = scrape()
    upload_to_sheets(df)
    logger.info('✓ FINISHED — LG rows: %d', len(df))


if __name__ == '__main__':
    main()
