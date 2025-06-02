from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.config import settings
from src.parser.base import BaseScraper


_CURRENCY_PAT = re.compile(r"\b(USDT|USD|\$|€|₽)\b", re.I)
_PRICE_NUM_PAT = re.compile(r"[\d\s]+[.,]?\d*")


def _parse_price(text: str) -> tuple[str, str]:
    """
    Вернёт (price_number, currency) из строки вида «6 399 USDT».
    Если не найдено – ('N/A', 'N/A')
    """
    cur_m = _CURRENCY_PAT.search(text)
    cur = cur_m.group(1).upper() if cur_m else "N/A"

    num_m = _PRICE_NUM_PAT.search(text.replace("\u202f", " "))
    if not num_m:
        return "N/A", cur
    # убираем пробелы / запятые → точку
    num = num_m.group(0).replace(" ", "").replace(",", ".")
    return num, cur


def _collect_categories(card) -> dict[str, str]:
    """Достаём буллеты «Special offer / NEW / In stock …»"""
    cats = [
        c.text.strip()
        for c in card.find_elements(By.CSS_SELECTOR, "a.card__categories div")
    ]
    return {
        "labels": "; ".join(
            c for c in cats if "special" in c.lower() or "new" in c.lower()
        ),
        "availability": next(
            (
                c
                for c in cats
                if any(k in c.lower() for k in ("stock", "sold", "пре-заказ"))
            ),
            "Unknown",
        ),
    }


class UminersScraper(BaseScraper):
    """Скрапер vitrina.uminers.com (ASIC-каталог)."""

    def __init__(
        self,
        url: str = settings.uminers_scraper.url_to_scrape,
        output_file: str = settings.uminers_scraper.output_file,
        max_workers: int = settings.uminers_scraper.max_workers,
    ):
        super().__init__([url])
        self.output_file = output_file
        self.max_workers = max_workers
        self.print_lock, self.data_lock = RLock(), RLock()

    def safe_print(self, msg: str) -> None:
        with self.print_lock:
            print(msg)

    def open_url(self, url):
        if not isinstance(url, str):
            url = str(url or "")
        if not url.strip():
            raise ValueError(
                "URL is empty – проверьте settings.uminers_scraper.url_to_scrape"
            )
        self.driver.get(url)
        self.safe_print(f"Открыта страница: {self.driver.title}")

    def extract_card_data(self, card) -> Dict[str, Any]:
        """Снимаем всё, что есть прямо в карточке."""
        data: Dict[str, Any] = {}

        # — название / URL
        title_a = card.find_element(By.CSS_SELECTOR, "h3.card__title a")
        data["name"] = title_a.text.strip()
        data["url"] = title_a.get_attribute("href")

        # — availability + спец-метки
        data.update(_collect_categories(card))

        # — hashrate / algorithm / payback
        for blk in card.find_elements(By.CSS_SELECTOR, "div.card__characteristic"):
            k = blk.find_element(
                By.CSS_SELECTOR, "div.card__characteristicName"
            ).text.strip()
            v = blk.find_element(
                By.CSS_SELECTOR, "div.card__characteristicValue"
            ).text.strip()
            if k.lower().startswith("hashrate"):
                data["hashrate"] = v
            elif k.lower().startswith("algorithm"):
                data["algorithm"] = v
            elif k.lower().startswith("payback"):
                data["payback"] = v

        # — crypto-иконки
        data["coins"] = ", ".join(
            ic.get_attribute("alt")
            for ic in card.find_elements(By.CSS_SELECTOR, ".card__crypto img")
        )

        # — цена
        price_raw = card.find_element(By.CSS_SELECTOR, "a.card__price").text.strip()
        data["price"], data["currency"] = _parse_price(price_raw)
        data["vat_included"] = "(с НДС)" in price_raw

        # — картинка
        try:
            img = card.find_element(By.CSS_SELECTOR, "a img")
            data["image_url"] = img.get_attribute("src")
        except Exception:
            data["image_url"] = None

        # — sold-out?
        data["sold_out"] = "-soldout" in card.get_attribute("class")
        return data

    @staticmethod
    def _extract_specs(driver) -> dict[str, str]:
        spec = {}
        for row in driver.find_elements(By.CSS_SELECTOR, "table.spec-table tr"):
            try:
                k = row.find_element(By.CSS_SELECTOR, "td.specL").text.strip()
                v = row.find_element(By.CSS_SELECTOR, "td.specV").text.strip()
                spec[k] = v
            except Exception:
                continue
        return spec

    def _pick_power(self, specs: dict[str, str]) -> str:
        """Power / Мощность независимо от языка."""
        for k, v in specs.items():
            if k.lower().startswith(("power", "мощность")):
                return v
        return "Unknown"

    def extract_product_cards(self) -> List[Dict[str, Any]]:
        """Собираем ВСЕ карточки на текущей странице."""
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "card__title"))
        )
        cards = self.driver.find_elements(
            By.CSS_SELECTOR,
            "div.catalog-item, div.card, div.product-card, div[data-card-product-id]",
        )
        self.safe_print(f"  → карточек на странице: {len(cards)}")
        return [self.extract_card_data(c) for c in cards]

    def _process_card(self, card: dict[str, Any]) -> dict[str, Any] | None:
        if not card["url"]:
            return None

        t0 = time.perf_counter()
        self.safe_print(f"  • {card['name']} → {card['url']}")

        drv = self.create_driver()
        try:
            drv.get(card["url"])
            WebDriverWait(drv, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "spec-table"))
            )
            specs = self._extract_specs(drv)
        finally:
            drv.quit()

        dt = time.perf_counter() - t0
        self.safe_print(f"    ↳ спецификация за {dt:0.1f}s")

        record = {
            "Модель": card["name"],
            "Алгоритм": card.get("algorithm", ""),
            "Хэшрейт": card.get("hashrate", specs.get("Хэшрейт", "Unknown")),
            "Потребление": self._pick_power(specs),
            "Цена": card["price"],
            "Валюта": card["currency"],
            "НДС": "с НДС" if card["vat_included"] else "без НДС",
            "Доступность": card["availability"],
            "Склад": (
                card["availability"].split(",")[-1].strip()
                if "," in card["availability"]
                else ""
            ),
            "Спец-метка": card["labels"],
            "Coins": card["coins"],
        }
        return record

    def _scan_catalog(self) -> list[dict]:
        raw, page = [], 1
        while True:
            self.safe_print(f"== Страница {page} ==")
            raw.extend(self.extract_product_cards())
            try:
                nxt = self.driver.find_element(
                    By.CSS_SELECTOR, 'a.pagination__item[rel="next"]:not(.disabled)'
                )
                nxt.click()
                WebDriverWait(self.driver, 10).until(EC.staleness_of(nxt))
                page += 1
            except Exception:
                break
        return raw

    def run(self) -> List[Dict[str, Any]]:
        self.safe_print(f"▶ Старт. Потоков: {self.max_workers}")
        self.open_url(self.urls[0])

        raw_cards = self._scan_catalog()
        self.safe_print(f"⛏  Найдено карточек всего: {len(raw_cards)}")

        products: list[dict] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(self._process_card, c): c for c in raw_cards}
            for fut in as_completed(futures):
                res = fut.result()
                if res:
                    with self.data_lock:
                        products.append(res)

        self._save_excel(products)
        self.safe_print(f"✅ Готово. Сохранено строк: {len(products)}")
        return products

    def _save_excel(self, rows: list[dict]) -> None:
        if not rows:
            self.safe_print("Нет данных для сохранения.")
            return
        df = pd.DataFrame(rows)
        p = (
            Path(settings.prepared_excels_dir).expanduser()
            / Path(self.output_file).name
        ).resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(p, index=False)
        self.safe_print(f"Файл сохранён: {p}")
