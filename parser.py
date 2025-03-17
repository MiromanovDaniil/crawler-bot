import os
import time
import random
import logging
import threading
from typing import Optional, List, Dict, Any

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename="logs/scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/90.0.4430.212 Safari/537.36",
]

class PriceScraper:
    def __init__(self, headless: bool = True, proxy: Optional[str] = None):
        """
        Initialize an undetected ChromeDriver with advanced anti-detection, 
        proxy rotation, and user agent rotation.
        """
        self.options = uc.ChromeOptions()
        
        # Rotate user-agent randomly
        user_agent = random.choice(USER_AGENTS)
        self.options.add_argument(f"user-agent={user_agent}")
        
        # Basic stealth options
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_argument("--disable-infobars")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--ignore-certificate-errors")
        self.options.add_argument("--allow-running-insecure-content")
        
        if headless:
            self.options.add_argument("--headless=new")
        
        # Set proxy if provided
        if proxy:
            self.options.add_argument(f'--proxy-server={proxy}')
        
        # Initialize the undetected ChromeDriver
        self.driver = uc.Chrome(options=self.options)
        
        # Optional: further mask automation flags
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        # Set a default implicit wait
        self.driver.implicitly_wait(10)
    
    def scrape_price(self, url: str, xpath: str, timeout: int = 30, retries: int = 3) -> Optional[float]:
        """
        Scrape the price from the given URL using the specified XPath.
        Includes retry logic and error handling.
        """
        attempt = 0
        while attempt < retries:
            try:
                logging.info(f"Scraping URL: {url} (Attempt {attempt+1})")
                self.driver.get(url)
                
                # Random sleep to mimic human behavior
                time.sleep(random.uniform(2, 4))
                
                price_element = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.XPATH, xpath))
                )
                
                self.driver.execute_script("arguments[0].scrollIntoView(true);", price_element)
                time.sleep(random.uniform(1, 2))
                
                price_text = self._clean_price(price_element.text)
                price_value = float(price_text)
                logging.info(f"Price scraped: {price_value}")
                return price_value
            
            except Exception as e:
                attempt += 1
                logging.error(f"Error scraping {url} on attempt {attempt}: {str(e)}")
                self._take_screenshot(f"error_{int(time.time())}")
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return None

    def _clean_price(self, text: str) -> str:
        """
        Clean and normalize the price text. Adapt the replacements as needed.
        """
        replacements = {
            ' ': '', 
            ' ': '',
            '₽': '',
            '€': '',
            '$': '',
            ',': '.',
            'р.': '',
            'руб.': '',
            'RUB': '',
            'EUR': '',
            'USD': ''
        }
        cleaned = text.strip()
        for old, new in replacements.items():
            cleaned = cleaned.replace(old, new)
            
        if '-' in cleaned:  # Handle price ranges by taking the first number
            cleaned = cleaned.split('-')[0]
        return cleaned

    def _take_screenshot(self, filename: str = 'error_screenshot'):
        """
        Save a screenshot in the 'screenshots' directory for debugging.
        """
        screenshots_dir = os.path.join(os.getcwd(), "screenshots")
        os.makedirs(screenshots_dir, exist_ok=True)
        screenshot_path = os.path.join(screenshots_dir, f"{filename}.png")
        try:
            self.driver.save_screenshot(screenshot_path)
            logging.info(f"Screenshot saved: {screenshot_path}")
        except Exception as e:
            logging.error(f"Screenshot failed: {str(e)}")
    
    def close(self):
        """Close the driver cleanly."""
        if self.driver:
            self.driver.quit()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @staticmethod
    def concurrent_scrape(urls_xpaths: List[Dict[str, Any]], headless: bool = True, proxy: Optional[str] = None) -> Dict[str, Optional[float]]:
        """
        Demonstrates concurrent scraping using threads.
        urls_xpaths: List of dictionaries with keys 'url' and 'xpath'.
        Returns a dictionary mapping URL to the scraped price.
        """
        results = {}
        threads = []

        def scrape_task(item):
            url = item["url"]
            xpath = item["xpath"]
            with PriceScraper(headless=headless, proxy=proxy) as scraper:
                price = scraper.scrape_price(url, xpath)
                results[url] = price

        for item in urls_xpaths:
            thread = threading.Thread(target=scrape_task, args=(item,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        return results


if __name__ == "__main__":
    products = [
        {
            "name": "Наушники Sony WH-1000XM4 черные (Ozon)",
            "url": "https://www.ozon.ru/product/naushniki-sony-wh-1000xm4-chernye-185538391/",
            "xpath": '/html/body/div[1]/div/div[1]/div[3]/div[3]/div[2]/div/div/div[1]/div[2]/div[1]/div[1]/div/div/div[1]/div[1]/button/span/div/div[1]/div/div/span'
        },
        {
            "name": "Наушники Sony WH-1000XM4 черные (Wildberries)",
            "url": "https://www.wildberries.ru/catalog/115704327/detail.aspx",
            "xpath": '//*[@id="7f2eeb67-ed8a-0969-a157-3b7153218473"]/div[3]/div[2]/div[2]/div/div/div/p/span/ins'
        },
        {
            "name": "Наушники Sony WH-1000XM4 черные (Avito 1)",
            "url": "https://www.avito.ru/krasnodar/audio_i_video/naushniki_sony_wh-1000xm4_4607999441",
            "xpath": '/html/body/div[1]/div/div[4]/div[1]/div/div[2]/div[3]/div/div[2]/div/div/div/div[1]/div/div[1]/div/div[1]/div/span/span/span[1]'
        },
        # {
        #     "name": "Наушники Sony WH-1000XM4 черные (Avito 2)",
        #     "url": "https://www.avito.ru/krasnodar/audio_i_video/sony_wh-1000xm4_4513429919",
        #     "xpath": '/html/body/div[1]/div/div[4]/div[1]/div/div[2]/div[3]/div/div[2]/div/div/div/div[1]/div/div[1]/div/div[1]/div/span/span/span[1]'
        # },
        # {
        #     "name": "Наушники Sony WH-1000XM4 черные (Avito 3)",
        #     "url": "https://www.avito.ru/krasnodar/audio_i_video/naushniki_sony_wh-1000xm4_4248595262",
        #     "xpath": '/html/body/div[1]/div/div[4]/div[1]/div/div[2]/div[3]/div/div[2]/div/div/div/div[1]/div/div[1]/div/div[1]/div/span/span/span[1]'
        # },
        {
            "name": "Наушники Sony WH-1000XM4 черные (Cifrus)",
            "url": "https://www.cifrus.ru/description/3/sony_wh-1000xm4_black",
            "xpath": '/html/body/div[1]/div[5]/div[4]/div[1]/div/div[1]/div[3]/div[5]/div/div[2]/div/div'
        },
        {
            "name": "Наушники Sony WH-1000XM4 черные (Doctorhead)",
            "url": "https://doctorhead.ru/product/sony_wh1000xm4_black/?srsltid=AfmBOoqMWeE8N3kcu1jO6phIuOrhzryDiJfEZoxeaoxpRWjAURn_dqHR",
            "xpath": '/html/body/div[2]/div[12]/div/div[4]/div[2]/div[1]/div[3]/div[1]/span'
        },
        {
            "name": "Наушники Sony WH-1000XM4 черные (DNS)",
            "url": "https://www.dns-shop.ru/product/00ac9144e05f1b80/besprovodnyeprovodnye-nausniki-sony-wh-1000xm4-cernyj/?utm_medium=organic&utm_source=google&utm_referrer=https%3A%2F%2Fwww.google.com%2F",
            "xpath": '/html/body/div[2]/div[2]/div[7]/div[1]/div/div[1]'
        },
        {
            "name": "Наушники Sony WH-1000XM4 черные (Pitergsm)",
            "url": "https://pitergsm.ru/catalog/audio/naushniki/besprovodnye-bluetooth/9329/",
            "xpath": '/html/body/div[2]/main/div/div[2]/div[1]/div[3]/div[1]/div[1]/div[1]/div[2]/span'
        },
        {
            "name": "Наушники Sony WH-1000XM4 черные (Yandex Market)",
            "url": "https://market.yandex.ru/product--wh-1000xm4/676030002?sku=100981243728&uniqueId=62878861&do-waremd5=ruvVjjAz668RCLiWtcez8A",
            "xpath": '/html/body/div[1]/div/div[2]/div/div/div/div[1]/div/div[1]/div[3]/div[3]/section[1]/div[1]/div[2]/div/div/div/div[2]/div[1]/div[2]/div[2]/div[1]/div/div/div[2]/div[1]/div/div[1]/span[2]/span[1]'
        },
        {
            "name": "Наушники Sony WH-1000XM4 черные (Biggeek)",
            "url": "https://biggeek.ru/products/besprovodnye-nausniki-sony-wh-1000xm4",
            "xpath": '/html/body/div[1]/main/div[2]/section[1]/div/div/div[2]/div/div[2]/div/div[2]/span[1]/span'
        }
    ]

    with PriceScraper(headless=False) as scraper:
        for product in products:
            price = scraper.scrape_price(product["url"], product["xpath"])
            print(f"{product['name']}: {price or 'ERROR'} RUB")