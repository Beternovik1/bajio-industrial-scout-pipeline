import logging
import os
import random
import re
import time
from typing import Optional, Tuple

import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

MAX_PAGES = int(os.getenv("MAX_PAGES_OCC", "5"))


class OCCScraper(BaseScraper):

    site = "occ"
    currency = "MXN"
    country_code = "MX"

    def __init__(self):
        self.session = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )

    # ------------------------------------------------------------------
    # PUBLIC ORCHESTRATION
    # ------------------------------------------------------------------

    def scrape(
        self,
        keywords: list,
        countries: list,
        is_remote: bool = False,
        max_results: int = 100,
    ) -> pd.DataFrame:
        df = self.scrape_metadata(keywords, countries, is_remote, max_results)
        if df.empty:
            return self.validate_output(df)

        urls = df["job_url"].tolist()
        enriched = self.enrich_descriptions(urls)

        descriptions = []
        for url in urls:
            data = enriched.get(url, {})
            descriptions.append(data.get("description"))

            salary_raw = data.get("salary_raw")
            if salary_raw:
                mask = df["job_url"] == url
                if mask.any() and df.loc[mask, "salary_min"].isna().all():
                    parsed_min, parsed_max = self._parse_salary(salary_raw)
                    df.loc[mask, "salary_min"] = parsed_min
                    df.loc[mask, "salary_max"] = parsed_max

        df["description"] = descriptions

        return self.validate_output(df)

    # ------------------------------------------------------------------
    # PHASE 1 — Metadata extraction from listing cards
    # ------------------------------------------------------------------

    def scrape_metadata(
        self,
        keywords: list,
        countries: list,
        is_remote: bool = False,
        max_results: int = 100,
    ) -> pd.DataFrame:
        records = []

        for keyword in keywords:
            keyword_slug = keyword.replace(" ", "-")
            base_url = "https://www.occ.com.mx"

            page = 1
            while True:
                if len(records) >= max_results:
                    logger.info("max_results (%s) reached for %s", max_results, keyword)
                    break

                if page > MAX_PAGES:
                    logger.info("MAX_PAGES_OCC (%s) reached for %s", MAX_PAGES, keyword)
                    break

                if is_remote:
                    url = f"{base_url}/empleos/de-{keyword_slug}/en-mexico-y-teletrabajo/?page={page}"
                else:
                    url = f"{base_url}/empleos/de-{keyword_slug}/?page={page}"

                logger.info("Phase 1 — Fetching page %s: %s", page, url)
                time.sleep(random.uniform(2, 4))

                try:
                    resp = self.session.get(url, timeout=30)
                    resp.raise_for_status()
                except Exception as e:
                    logger.warning("Request failed for %s: %s", url, e)
                    break

                soup = BeautifulSoup(resp.text, "html.parser")

                cards = soup.select("div[data-offers-grid-offer-item-container]")
                if not cards:
                    logger.info("No job cards found — stopping pagination for %s", keyword)
                    break

                for card in cards:
                    if len(records) >= max_results:
                        break

                    title_tag = card.select_one("h2.text-grey-900.text-lg")
                    title = title_tag.get_text(strip=True) if title_tag else None

                    data_id = card.get("data-id")
                    job_url = f"https://www.occ.com.mx/empleo/oferta/{data_id}/" if data_id else None

                    salary_min, salary_max = None, None
                    salary_tag = card.select_one("span.mr-2.text-grey-900.font-base.font-light")
                    if salary_tag:
                        salary_text = salary_tag.get_text(strip=True)
                        if "$" in salary_text:
                            salary_min, salary_max = self._parse_salary(salary_text)

                    # company = None
                    company_container = card.select_one("div.col-span-10 div.h-\\[21px\\]")
                    company = company_container.get_text(strip=True) if company_container else None


                    raw_location = None
                    location_tag = card.select_one("div.no-alter-loc-text p")
                    if location_tag:
                        raw_location = location_tag.get_text(strip=True)

                    date_posted = None
                    date_tag = card.select_one("span.mr-2.text-sm.font-light")
                    if date_tag:
                        date_posted = date_tag.get_text(strip=True)

                    job_type = "Remoto" if is_remote else None

                    records.append({
                        "site": self.site,
                        "job_url": job_url,
                        "title": title,
                        "company": company,
                        "raw_location": raw_location,
                        "description": None,
                        "date_posted": date_posted,
                        "salary_min": salary_min,
                        "salary_max": salary_max,
                        "currency": self.currency,
                        "job_type": job_type,
                        "country_code": self.country_code,
                    })

                page += 1

        return pd.DataFrame(records) if records else pd.DataFrame(columns=[
            "site", "job_url", "title", "company", "raw_location",
            "description", "date_posted", "salary_min", "salary_max",
            "currency", "job_type", "country_code"
        ])

    # ------------------------------------------------------------------
    # PHASE 2 — Description enrichment from detail pages
    # ------------------------------------------------------------------

    def enrich_descriptions(self, job_urls: list) -> dict:
        result = {}

        for url in job_urls:
            logger.info("Phase 2 — Enriching: %s", url)
            time.sleep(random.uniform(3, 6))

            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                logger.warning("Failed to enrich %s: %s", url, e)
                result[url] = {"description": None, "salary_raw": None}
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            # 1. Intentamos los selectores clásicos primero
            desc_tag = soup.select_one("#Descripcion") or soup.find("div", {"itemprop": "description"})
            
            # 2. Si no funcionan, aplicamos el anclaje por texto basado en la nueva estructura de OCC
            if not desc_tag:
                desc_header = soup.find(lambda tag: tag.name == "p" and "Descripción:" in tag.get_text(strip=True))
                if desc_header:
                    desc_tag = desc_header.find_parent("div")
                else:
                    # 3. Fallback final al contenedor de Tailwind
                    desc_tag = soup.select_one("div.break-words")

            description = desc_tag.get_text(separator="\n", strip=True) if desc_tag else None

            salary_raw = None
            page_text = soup.get_text(separator=" ", strip=True)
            salary_match = re.search(
                r'\$\s*[\d,]+\.?\d*(?:\s*[-–]\s*\$?\s*[\d,]+\.?\d*)?',
                page_text,
            )
            if salary_match:
                salary_raw = salary_match.group()

            result[url] = {"description": description, "salary_raw": salary_raw}

        return result

    # ------------------------------------------------------------------
    # INTERNAL HELPERS
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_salary(text: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
        if not text:
            return None, None
        numbers = re.findall(r"\$?\s*([\d,]+\.?\d*)", text)
        numbers = [float(n.replace(",", "")) for n in numbers]
        if not numbers:
            return None, None
        if len(numbers) == 1:
            return numbers[0], numbers[0]
        return numbers[0], numbers[1]


# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

#     scraper = OCCScraper()

#     print("--- PHASE 1: Fast metadata extraction (simulates GitHub Actions) ---")
#     df_phase1 = scraper.scrape_metadata(
#         keywords=["data engineer"],
#         countries=["MX"],
#         is_remote=False,
#         max_results=3,
#     )
#     print(f"Shape: {df_phase1.shape}")
#     print(df_phase1.head(3).to_string())

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    scraper = OCCScraper()

    print("--- INICIANDO EXTRACCIÓN COMPLETA (FASE 1 + FASE 2) ---")
    # Usamos el método orquestador principal
    df_final = scraper.scrape(
        keywords=["data analyst"],
        countries=["MX"],
        is_remote=False,
        max_results=1,
    )
    
    print(f"Shape final: {df_final.shape}")
    print(df_final[["title", "company", "description", "salary_min", "salary_max"]].head(3).to_string())