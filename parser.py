import os
import time
import random
import logging
import threading
from typing import Optional, List, Dict, Any

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(log_dir, "scraper.log"),
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
        Initializes the Chrome WebDriver with selenium-stealth to bypass automation detection.
        """
        options = webdriver.ChromeOptions()
        
        user_agent = random.choice(USER_AGENTS)
        options.add_argument(f"user-agent={user_agent}")
        
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--allow-running-insecure-content")
        
        if headless:
            options.add_argument("--headless=new")
        
        if proxy:
            options.add_argument(f'--proxy-server={proxy}')
        
        self.driver = webdriver.Chrome(options=options)
        
        stealth(self.driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True)
        
        self.driver.implicitly_wait(10)
    
    def scrape_price(self, url: str, xpath: str, timeout: int = 30, retries: int = 3) -> Optional[float]:
        """
        Scrapes the price from a given URL using the specified XPath.
        Includes retry logic and error handling.
        """
        attempt = 0
        while attempt < retries:
            try:
                logging.info(f"Scraping URL: {url} (Attempt {attempt+1})")
                self.driver.get(url)

                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

                time.sleep(random.uniform(3, 6))

                def wait_for_element(driver):
                    try:
                        elem = driver.find_element(By.XPATH, xpath)
                        return elem if elem.text.strip() != "" else False
                    except Exception:
                        return False

                price_element = WebDriverWait(self.driver, timeout).until(wait_for_element)

                self.driver.execute_script("arguments[0].scrollIntoView(true);", price_element)
                time.sleep(random.uniform(1, 2))

                # Attempt to retrieve text using multiple attributes if necessary
                price_text = price_element.text.strip() or \
                            price_element.get_attribute("innerText").strip() or \
                            price_element.get_attribute("textContent").strip()

                cleaned_price = self._clean_price(price_text)
                price_value = float(cleaned_price)
                logging.info(f"Scraped price: {price_value}")
                return price_value

            except Exception as e:
                attempt += 1
                logging.error(f"Error scraping {url} on attempt {attempt}: {str(e)}")
                self._take_screenshot(f"error_{int(time.time())}")
                time.sleep(2 ** attempt)  # Exponential backoff before retry

        return None


    def _clean_price(self, text: str) -> str:
        """
        Cleans and normalizes the price text obtained via .text.
        Removes regular spaces, non-breaking spaces, thin spaces, and currency symbols.
        """
        replacements = {
            '\xa0': '',      # non-breaking space
            '\u202f': '',    # thin non-breaking space
            '\u2006': '',
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
        
        if '-' in cleaned:  # Handle price ranges by selecting the first number
            cleaned = cleaned.split('-')[0]
        return cleaned


    def _take_screenshot(self, filename: str = 'error_screenshot'):
        """
        Saves a screenshot in the 'screenshots' directory for debugging.
        """
        screenshots_dir = os.path.join(os.getcwd(), "screenshots")
        os.makedirs(screenshots_dir, exist_ok=True)
        screenshot_path = os.path.join(screenshots_dir, f"{filename}.png")
        try:
            self.driver.save_screenshot(screenshot_path)
            logging.info(f"Screenshot saved: {screenshot_path}")
        except Exception as e:
            logging.error(f"Failed to save screenshot: {str(e)}")
    
    def close(self):
        """Properly closes the WebDriver."""
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
        urls_xpaths: List of dictionaries with 'url' and 'xpath' keys.
        Returns a dictionary mapping URLs to scraped prices.
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
            "xpath": "//span[contains(@class, 'z0l_29') and contains(@class, 'yl9_29')]"
        },
        {
            "name": "Sony WH-1000XM4 Black Headphones (Wildberries)",
            "url": "https://www.wildberries.ru/catalog/115704327/detail.aspx",
            "xpath": "//ins[contains(@class, 'price-block__final-price') and contains(@class, 'red-price')]"
        },
        {
            "name": "Наушники Sony WH-1000XM4 черные (Avito 1)",
            "url": "https://www.avito.ru/krasnodar/audio_i_video/naushniki_sony_wh-1000xm4_4607999441",
            "xpath": "//span[@itemprop='price' and @data-marker='item-view/item-price']"
        },
        {
            "name": "Наушники Sony WH-1000XM4 черные (Avito 2)",
            "url": "https://www.avito.ru/krasnodar/audio_i_video/sony_wh-1000xm4_4513429919",
            "xpath": "//span[@itemprop='price' and @data-marker='item-view/item-price']"
        },
        {
            "name": "Наушники Sony WH-1000XM4 черные (Avito 3)",
            "url": "https://www.avito.ru/krasnodar/audio_i_video/naushniki_sony_wh-1000xm4_4248595262",
            "xpath": "//span[@itemprop='price' and @data-marker='item-view/item-price']"
        },
        {
            "name": "Наушники Sony WH-1000XM4 черные (Cifrus)",
            "url": "https://www.cifrus.ru/description/3/sony_wh-1000xm4_black",
            "xpath": "//div[@class='new-price ']"
        },
        {
            "name": "Наушники Sony WH-1000XM4 черные (DNS)",
            "url": "https://www.dns-shop.ru/product/00ac9144e05f1b80/besprovodnyeprovodnye-nausniki-sony-wh-1000xm4-cernyj/?utm_medium=organic&utm_source=google&utm_referrer=https%3A%2F%2Fwww.google.com%2F",
            "xpath": "//div[@class='product-buy__price']"
        },
        {
            "name": "Наушники Sony WH-1000XM4 черные (Yandex Market)",
            "url": "https://market.yandex.ru/product--wh-1000xm4/676030002?sku=100981243728&uniqueId=62878861&do-waremd5=ruvVjjAz668RCLiWtcez8A",
            "xpath": "(//span[@class='ds-valueLine' and @data-auto='snippet-price-current']/span[1])[1]"
        },
        {
            "name": "Наушники Sony WH-1000XM4 черные (Biggeek)",
            "url": "https://biggeek.ru/products/besprovodnye-nausniki-sony-wh-1000xm4",
            "xpath": "//span[@class='total-prod-price']"
        }
    ]

    with PriceScraper(headless=False) as scraper:
        for product in products:
            price = scraper.scrape_price(product["url"], product["xpath"])
            print(f"{product['name']}: {price or 'ERROR'} RUB")
