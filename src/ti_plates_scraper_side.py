#!/usr/bin/env python3

# src/scrape.py
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from datetime import datetime
import csv
from pathlib import Path
import time
import os

def main():
    # GitHub Actions uses proxy, local doesn't
    proxy_enabled = os.getenv('USE_PROXY', 'false').lower() == 'true'
    driver = setup_stealth_chrome(proxy_enabled=proxy_enabled)

    output_dir = Path("outputs")
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = output_dir / f"carie_auktion_side_{timestamp}.csv"

    try:
        # TEST PROXY FIRST
        driver.set_page_load_timeout(20)
        print("Testing alternative Ticino site...")
        driver.get("https://www.auktion-ch.ch/auktion/ti/default.aspx")
        time.sleep(3)  # Human-like load time

        print("=== DEBUG ===")
        print("Title:", driver.title)
        print("URL after load:", driver.current_url)
        print("Page source length:", len(driver.page_source))
        print("Contains 'tabContent':", 'tabContent' in driver.page_source)

        # Save HTML for inspection
        with open("debug_page.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print("Debug HTML saved")

        # Scraping logic
        plate_numbers = []
        starting_prices = []
        min_increments = []
        current_offers = []
        time_to_go_list = []
        offers_numbers = []

        row_index = 1

        # Scroll a bit to trigger lazy loading
        driver.execute_script("window.scrollTo(0, 200);")
        time.sleep(1)
    
        while row_index <= 5:
            try:
                print(f"Trying to scrape row {row_index}...")
                plate_numbers.append(driver.find_element(By.XPATH, f'//*[@id="CAR"]/table/tbody/tr[{row_index}]/th[2]/a/div').get_attribute('innerText'))
                starting_prices.append("N/A")
                min_increments.append("N/A")
                current_offers.append(driver.find_element(By.XPATH, f'//*[@id="CAR"]/table/tbody/tr[{row_index}]/th[4]').get_attribute('innerText'))
                time_to_go_list.append(driver.find_element(By.XPATH, f'//*[@id="CAR"]/table/tbody/tr[{row_index}]/th[3]').get_attribute('innerText'))
                offers_numbers.append(driver.find_element(By.XPATH, f'//*[@id="CAR"]/table/tbody/tr[{row_index}]/th[5]/span[1]').get_attribute('innerText'))

                row_index += 1
            except:
                break

        print(f"Scraped {len(plate_numbers)} auction plates.")
        
        row_index_fixed = 1
        # Fixed price plates
        #(driver.find_element(By.ID, 'tab3')).click()
        # Skip clicking entirely - call the JS function directly
        tab_link = driver.find_element(By.CSS_SELECTOR, 'a[href="#CARPLATESLIST"][data-toggle="tab"]').click()
        time.sleep(2)  # Give JS time to run

        while True:
            try:
                print(f"Trying to scrape fixed price row {row_index_fixed}...")
                plate_numbers.append(driver.find_element(By.XPATH, f'//*[@id="CARPLATESLIST"]/table/tbody/tr[{row_index_fixed}]/th[2]/a/div').get_attribute('innerText'))
                starting_prices.append(driver.find_element(By.XPATH, f'//*[@id="CARPLATESLIST"]/table/tbody/tr[{row_index_fixed}]/th[4]').get_attribute('innerText'))
                min_increments.append("N/A")
                current_offers.append("N/A")
                time_to_go_list.append("N/A")
                offers_numbers.append("N/A")

                row_index_fixed += 1
            except:
                print(f"Scraped {len(plate_numbers)} plates.")
                break

    finally:
        driver.quit()

    # Write file
    with out_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["plate_number", "starting_price", "min_increment", "current_offer", "time_to_go", "offers_number"]
        )
        for row in zip(
            plate_numbers,
            starting_prices,
            min_increments,
            current_offers,
            time_to_go_list,
            offers_numbers,
        ):
            plate, start, min_inc, cur, tgo, offers = row
            plate_clean = clean_plate(plate)
            cleaned_row = (plate_clean, start, min_inc, cur, tgo, offers)
            writer.writerow(cleaned_row)
            print(f"Wrote row: {cleaned_row}")
            
def clean_plate(value: str) -> str:
    if value is None:
        return ""
    # Remove the dot-like separator and surrounding spaces
    # "TI • 566" -> "TI 566"
    value = value.replace("•", " ")
    # Normalize multiple spaces
    value = re.sub(r"\s+", " ", value)
    return value.strip()

def setup_stealth_chrome(proxy_enabled=False):
    options = Options()

    # Core headless + CI options
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    # Stealth options
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # FREE SWISS PROXY (HTTP - reliable)
    # PROXY ONLY for GitHub Actions (US IP)
    if proxy_enabled:
        proxy = "81.62.179.218:5472"  # Updated working proxy
        options.add_argument(f"--proxy-server=http://{proxy}")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    # Stealth scripts
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
    driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['it-CH', 'it', 'en']})")
    
    return driver

if __name__ == "__main__":
    main()
