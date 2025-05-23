from typing import List, Dict, Any
import time
import pandas as pd
from pathlib import Path

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import RLock

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from src.parser.base import BaseScraper
from src.config import settings


class UminersScraper(BaseScraper):
    def __init__(
        self,
        urls: List[str],
        output_file: str = settings.uminers_scraper.output_file,
        max_workers: int = settings.uminers_scraper.max_workers,
    ):
        # Initialize the base scraper
        super().__init__(urls)
        self.output_file = output_file
        # Number of parallel workers
        self.max_workers = max_workers
        self.all_products_data = []
        # Lock for thread-safe printing
        self.print_lock = RLock()
        # Lock for thread-safe data access
        self.data_lock = RLock()

    def safe_print(self, message: str):
        """Thread-safe printing"""
        with self.print_lock:
            print(message)

    def open_url(self, url: str):
        """Open a specific URL and return the page title"""
        self.driver.get(url)
        # Print the title of the page
        self.safe_print(f"Заголовок страницы: {self.driver.title}")

    def extract_card_data(self, card) -> Dict[str, Any]:
        """Extract data from a specific product card element"""
        product_data = {}

        # Extract product name
        try:
            product_name_elem = card.find_element(By.CSS_SELECTOR, "h3.card__title a")
            product_data["name"] = product_name_elem.text.strip()
            product_data["url"] = product_name_elem.get_attribute("href")
        except Exception as e:
            self.safe_print(f"⚠️ Ошибка при извлечении названия продукта: {e}")
            product_data["name"] = "Unknown"
            product_data["url"] = None

        # Extract availability status
        try:
            availability_elem = card.find_element(
                By.CSS_SELECTOR, "div.card__category_white"
            )
            product_data["availability"] = availability_elem.text.strip()
        except Exception as e:
            self.safe_print(f"⚠️ Ошибка при извлечении данных о доступности: {e}")
            product_data["availability"] = "Unknown"

        # Extract characteristics from the card
        characteristics = {}
        try:
            char_elems = card.find_elements(By.CSS_SELECTOR, "div.card__characteristic")
            for char_elem in char_elems:
                name_elem = char_elem.find_element(
                    By.CSS_SELECTOR, "div.card__characteristicName"
                )
                value_elem = char_elem.find_element(
                    By.CSS_SELECTOR, "div.card__characteristicValue"
                )
                characteristics[name_elem.text.strip()] = value_elem.text.strip()
        except Exception as e:
            self.safe_print(f"⚠️ Ошибка при извлечении характеристик: {e}")

        product_data["characteristics"] = characteristics

        # Extract price, including currency and VAT status
        try:
            # Try the original selector
            price_elems = card.find_elements(By.CSS_SELECTOR, "a.cardprice")

            # If not found, try with alternative selectors
            if not price_elems:
                price_elems = card.find_elements(By.CSS_SELECTOR, "a.card__price")

            if price_elems:
                price_elem = price_elems[0]
                price_text = price_elem.text.strip()
                product_data["price"] = price_text

                # Extract currency
                currency = "₽"  # Default currency
                if "₽" in price_text:
                    currency = "₽"
                elif "$" in price_text:
                    currency = "$"
                elif "€" in price_text:
                    currency = "€"
                product_data["currency"] = currency

                # Check for VAT inclusion
                vat_included = False
                if "(с НДС)" in price_text:
                    vat_included = True
                product_data["vat_included"] = vat_included
            else:
                self.safe_print("⚠️ Цена не найдена с помощью любого селектора")
                product_data["price"] = "Price not available"
                product_data["currency"] = "Unknown"
                product_data["vat_included"] = False

        except Exception as e:
            self.safe_print(f"⚠️ Ошибка при извлечении цены: {e}")
            product_data["price"] = "Price not available"
            product_data["currency"] = "Unknown"
            product_data["vat_included"] = False

        # Extract image URL
        try:
            img_elem = card.find_element(By.CSS_SELECTOR, "a img")
            product_data["image_url"] = img_elem.get_attribute("src")
        except Exception as e:
            self.safe_print(f"⚠️ Ошибка при извлечении URL изображения: {e}")
            product_data["image_url"] = None

        return product_data

    def extract_detailed_specifications(self) -> Dict[str, str]:
        """Extract detailed specifications from the product page"""
        specifications = {}

        wait = WebDriverWait(self.driver, 10)
        try:
            # Wait for specifications table to load
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "spec-table")))

            # Extract all specs from the table
            spec_rows = self.driver.find_elements(
                By.CSS_SELECTOR, "table.spec-table tr"
            )

            for row in spec_rows:
                try:
                    spec_name = row.find_element(
                        By.CSS_SELECTOR, "td.specL"
                    ).text.strip()
                    spec_value = row.find_element(
                        By.CSS_SELECTOR, "td.specV"
                    ).text.strip()
                    specifications[spec_name] = spec_value
                except Exception as e:
                    self.safe_print(f"⚠️ Ошибка при извлечении ряда спецификаций: {e}")
                    continue

        except Exception as e:
            self.safe_print(f"⚠️ Ошибка при извлечении таблицы спецификаций: {e}")

        return specifications

    def extract_product_cards(self) -> List[Dict[str, Any]]:
        """Extract data from all product cards on the page"""
        wait = WebDriverWait(self.driver, 10)

        try:
            # Wait for products to load
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "card__title")))

            products = []
            # Find all product cards
            product_cards = self.driver.find_elements(
                By.CSS_SELECTOR, "div.catalog-item, div.card, div.product-card"
            )

            if not product_cards:
                self.safe_print(
                    "No product cards found. Trying alternative selectors..."
                )
                # Try alternative selectors
                product_cards = self.driver.find_elements(
                    By.CSS_SELECTOR,
                    ".product-card, .catalog-item, div[data-card-product-id]",
                )

            self.safe_print(f"Found {len(product_cards)} product cards")

            for card in product_cards:
                product_data = self.extract_card_data(card)
                products.append(product_data)

            return products

        except Exception as e:
            self.safe_print(f"⚠️ Ошибка при извлечении карточек продуктов: {e}")
            return []

    def process_product(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single product in a separate thread"""
        # Create a new WebDriver instance for this thread
        driver = self.create_driver()

        try:
            if not product["url"]:
                return None

            self.safe_print(f"\nОбработка: {product['name']}")
            # Navigate to product details page
            driver.get(product["url"])
            time.sleep(2)  # Wait for page to load

            # Extract detailed specifications
            wait = WebDriverWait(driver, 10)
            try:
                # Wait for specifications table to load
                wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "spec-table"))
                )

                # Extract all specs from the table
                spec_rows = driver.find_elements(By.CSS_SELECTOR, "table.spec-table tr")

                detailed_specs = {}
                for row in spec_rows:
                    try:
                        spec_name = row.find_element(
                            By.CSS_SELECTOR, "td.specL"
                        ).text.strip()
                        spec_value = row.find_element(
                            By.CSS_SELECTOR, "td.specV"
                        ).text.strip()
                        detailed_specs[spec_name] = spec_value
                    except Exception as e:
                        self.safe_print(f"Ошибка при извлечении ряда: {e}")
                        continue

            except Exception as e:
                self.safe_print(f"⚠️ Ошибка при извлечении таблицы спецификаций: {e}")
                detailed_specs = {}

            # Get price and currency information
            price = product.get("price", "Price not available")
            currency = product.get("currency", "₽")
            vat_included = product.get("vat_included", False)
            vat_status = "с НДС" if vat_included else "без НДС"

            # Extract required fields
            manufacturer = detailed_specs.get("Производитель", "Unknown")
            model = detailed_specs.get("Модель", "Unknown")
            if model == "−" or not model:
                # Use the product name if model is not available
                model = product.get("name", "Unknown").replace(manufacturer, "").strip()

            combined_model = f"{manufacturer} {model}".strip()

            hashrate = detailed_specs.get("Хэшрейт", "Unknown")
            power_consumption = detailed_specs.get("Мощность", "Unknown")

            # Extract price value from the price string
            # Remove currency symbol and VAT info
            clean_price = re.sub(r"[₽$€]|\(.*?\)", "", price).strip()
            if "−" in clean_price:
                clean_price = "N/A"

            # Create a record for Excel
            product_record = {
                "Модель": combined_model,
                "Хэшрейт": hashrate,
                "Потребление": power_consumption,
                "Цена": clean_price,
                "Валюта": currency,
                "НДС": vat_status,
            }

            self.safe_print(
                f"Добавлен продукт: {combined_model}, Hashrate: {hashrate}, Power: {power_consumption}, "
                f"Price: {clean_price} {currency} ({vat_status})"
            )

            return product_record

        except Exception as e:
            self.safe_print(
                f"⚠️ Ошибка при обработке продукта {product.get('name', 'Unknown')}: {e}"
            )
            return None
        finally:
            driver.quit()

    def process_all_urls(self):
        """Process all URLs, extract data from all product cards, and save to Excel"""
        all_products = []
        all_product_cards = []

        # First, collect all product cards from all URLs sequentially
        for url in self.urls:
            self.safe_print(f"\nОбрабатывается URL: {url}")
            self.open_url(url)

            # Extract all product cards on this page
            products = self.extract_product_cards()

            # Store all product cards for later parallel processing
            all_product_cards.extend(products)

        self.safe_print(f"\nВсего найдено карточек продуктов: {len(all_product_cards)}")

        # Process product cards in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all product processing tasks
            future_to_product = {
                executor.submit(self.process_product, product): product
                for product in all_product_cards
            }

            # Process results as they complete
            for future in as_completed(future_to_product):
                product = future_to_product[future]
                try:
                    result = future.result()
                    if result:
                        # Thread-safe update of all_products
                        with self.data_lock:
                            all_products.append(result)
                except Exception as e:
                    self.safe_print(
                        f"Продукт: {product.get('name', 'Unknown')} при обработке вызвал исключение: {e}"
                    )

        # Save all products to Excel
        self.save_to_excel(all_products)

        return all_products

    def save_to_excel(self, products):
        if not products:
            self.safe_print("Нет продуктов для сохранения в Excel")
            return

        try:
            df = pd.DataFrame(products)

            # 1. Catalog -> absolute Path
            save_dir = Path(settings.prepared_excels_dir).expanduser().resolve()
            save_dir.mkdir(parents=True, exist_ok=True)

            # 2. File name
            file_name = Path(self.output_file).name
            full_path = save_dir / file_name

            # 3. Save to Excel
            df.to_excel(full_path, index=False)
            self.safe_print(
                f"\n✅ Успешно сохранено {len(products)} продуктов в {full_path}"
            )
        except Exception as e:
            self.safe_print(f"⚠️ Ошибка при сохранении в Excel: {e}")

            try:
                csv_path = full_path.with_suffix(".csv")
                df.to_csv(csv_path, index=False)
                self.safe_print(f"Сохранено в CSV: {csv_path}")
            except Exception as csv_error:
                self.safe_print(f"⚠️ Ошибка при сохранении в CSV: {csv_error}")

    def run(self):
        """Main method to run the scraper"""
        try:
            self.safe_print(
                f"Начинается обработка {len(self.urls)} URLs с {self.max_workers} воркерами"
            )
            products = self.process_all_urls()
            self.safe_print(f"Сборка завершена. Обработано {len(products)} продуктов.")
            return products
        except Exception as e:
            self.safe_print(f"⚠️ Ошибка при запуске скрапера: {e}")
            return []
        finally:
            self.safe_print("Закрывается браузер...")
            if hasattr(self, "driver") and self.driver:
                self.driver.quit()
