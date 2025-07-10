import sys, os, re, json, base64, datetime, time, logging
import pandas as pd, gspread, pytz
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from gspread_formatting import format_cell_ranges, CellFormat, TextFormat, Color

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

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

driver = get_driver()
driver.get("https://www.amazon.de/")
wait = WebDriverWait(driver, 20)

# 1) 배송지 클릭 (위쪽 네비게이션바)
try:
    location_btn = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="nav-global-location-popover-link"]')))
    location_btn.click()
    print("📍 배송지 버튼 클릭 완료")
except:
    print("❌ 배송지 버튼 클릭 실패")

# 2) 우편번호 입력
try:
    zip_input = wait.until(EC.presence_of_element_located((By.ID, "GLUXZipUpdateInput")))
    zip_input.clear()
    zip_input.send_keys("65760")
    print("📮 우편번호 입력 완료")
except:
    print("❌ 우편번호 입력 실패")

# 3) 적용 버튼 클릭
try:
    wait.until(EC.element_to_be_clickable((By.ID, "GLUXZipUpdate"))).click()
    print("📦 우편번호 적용 클릭 완료")
    time.sleep(2)
except:
    print("❌ 적용 버튼 클릭 실패")

# 4) 새로고침
driver.refresh()
time.sleep(3)

try:
    ship_to = wait.until(EC.presence_of_element_located((By.ID, "glow-ingress-line2"))).text
    print("✅ 현재 배송지:", ship_to)
except:
    print("❌ 배송지 확인 실패")

