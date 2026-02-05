# src/scrape.py
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from datetime import datetime
import csv
from pathlib import Path
import time

def main():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )

    output_dir = Path("outputs")
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = output_dir / f"carie_auktion_{timestamp}.csv"

    try:
        driver.get("https://www.carieauktion.ti.ch/ecari-auktion/ui/app/init")
        print("Title:", driver.title)
        # Scraping logic
        plate_numbers = []
        starting_prices = []
        min_increments = []
        current_offers = []
        time_to_go_list = []
        offers_numbers = []

        row_index = 1
    
        while True:
            try:
                td_amount = driver.find_element(By.XPATH, f'//*[@id="tabContent1"]/div/div/div[1]/table/tbody/tr[{row_index}]/td[2]')
                
                print(f"DEBUG row {row_index}:")
                print(f"  .text: '{td_amount.text}'")
                print(f"  innerText: '{td_amount.get_attribute('innerText')}'")
                print(f"  textContent: '{td_amount.get_attribute('textContent')}'")
                print(f"  innerHTML: '{td_amount.get_attribute('innerHTML')}'")
                print(f"  Visible: {td_amount.is_displayed()}")
                
                # Use the first non-empty value
                text = td_amount.text or td_amount.get_attribute('innerText') or td_amount.get_attribute('textContent')
                starting_prices.append(text.strip())
                
            except Exception as e:
                print(f"Failed: {e}")
            try:
                plate_numbers.append(driver.find_element(By.XPATH, f'//*[@id="tabContent1"]/div/div/div[1]/table/tbody/tr[{row_index}]/td[1]/a/div/div[4]').get_attribute('innerText'))
                starting_prices.append(driver.find_element(By.XPATH, f'//*[@id="tabContent1"]/div/div/div[1]/table/tbody/tr[{row_index}]/td[2]').get_attribute('innerText'))
                min_increments.append(driver.find_element(By.XPATH, f'//*[@id="tabContent1"]/div/div/div[1]/table/tbody/tr[{row_index}]/td[3]').get_attribute('innerText'))
                current_offers.append(driver.find_element(By.XPATH, f'//*[@id="tabContent1"]/div/div/div[1]/table/tbody/tr[{row_index}]/td[4]').get_attribute('innerText'))
                time_to_go_list.append(driver.find_element(By.XPATH, f'//*[@id="tabContent1"]/div/div/div[1]/table/tbody/tr[{row_index}]/td[5]').get_attribute('innerText'))
                offers_numbers.append(driver.find_element(By.XPATH, f'//*[@id="tabContent1"]/div/div/div[1]/table/tbody/tr[{row_index}]/td[6]').get_attribute('innerText'))

                row_index += 7
            except:
                break

        print(f"Scraped {len(plate_numbers)} auction plates.")
        
        row_index_fixed = 1
        # Fixed price plates
        #(driver.find_element(By.ID, 'tab3')).click()
        # Skip clicking entirely - call the JS function directly
        driver.execute_script("selectTab('3');")
        time.sleep(2)  # Give JS time to run

        while True:
            try:
                plate_numbers.append(driver.find_element(By.XPATH, f'//*[@id="tabContent3"]/div/div[1]/div/div[{row_index_fixed}]/div[1]/div[4]').get_attribute('innerText'))
                starting_prices.append(driver.find_element(By.XPATH, f'//*[@id="tabContent3"]/div/div[1]/div/div[{row_index_fixed}]/div[3]').get_attribute('innerText'))
                min_increments.append("N/A")
                current_offers.append("N/A")
                time_to_go_list.append("N/A")
                offers_numbers.append("N/A")

                row_index_fixed += 1
            except:
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
            writer.writerow(row)

if __name__ == "__main__":
    main()
