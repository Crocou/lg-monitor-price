from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")  # UI 확인용, headless 제거
    options.add_argument("--lang=de-DE")
    driver = webdriver.Chrome(options=options)
    return driver

driver = get_driver()
driver.get("https://www.amazon.de/")
wait = WebDriverWait(driver, 20)
