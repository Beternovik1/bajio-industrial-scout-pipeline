# src/scrapers/computrabajo_scraper.py
import logging
import os
import random
import re
import time
from typing import Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

# Playwright could be used later as a fallback if blocking increases:
# from playwright.sync_api import sync_playwright

from base_scraper import BaseScraper

logger = logging.getLogger(__name__)

COUNTRY_CONFIG = {
    "MX": {"subdomain": "mx", "currency": "MXN"},
    "CO": {"subdomain": "co", "currency": "COP"},
    "PE": {"subdomain": "pe", "currency": "PEN"},
    "CL": {"subdomain": "cl", "currency": "CLP"},
    "AR": {"subdomain": "ar", "currency": "ARS"},
}

MAX_PAGES = int(os.getenv("MAX_PAGES_COMPUTRABAJO", "5"))


class ComputrabajoScraper(BaseScraper):

    site = "computrabajo"

    def __init__(self):
        self.ua = UserAgent()
        # NOTE: Do NOT set User-Agent on the session directly.
        # _get_headers() rotates it on every request instead.
        self.session = requests.Session()

    def _get_headers(self) -> dict:
        """Rotate User-Agent and add realistic browser headers on every call."""
        return {
            "User-Agent": self.ua.random,
            "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }

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
        """
        All-in-one convenience method: runs Phase 1 then Phase 2,
        merges descriptions, validates output, and returns.

        NOTE: For distributed production, call scrape_metadata() and
        enrich_descriptions() separately via a database queue so Phase 1
        runs on fast datacenter IPs (GitHub Actions) and Phase 2 runs on
        residential IPs to avoid detection on detail pages.
        """
        # Phase 1 — no validate_output here, done at the end
        df = self.scrape_metadata(keywords, countries, is_remote, max_results)
        if df.empty:
            return self.validate_output(df)

        # Phase 2 — enrich descriptions
        urls = df["job_url"].tolist()
        enriched = self.enrich_descriptions(urls)

        descriptions = []
        for url in urls:
            data = enriched.get(url, {})
            descriptions.append(data.get("description"))

            # Recover salary from detail page if missing in Phase 1
            salary_raw = data.get("salary_raw")
            if salary_raw:
                mask = df["job_url"] == url
                if mask.any() and df.loc[mask, "salary_min"].isna().all():
                    parsed_min, parsed_max = self._parse_salary(salary_raw)
                    df.loc[mask, "salary_min"] = parsed_min
                    df.loc[mask, "salary_max"] = parsed_max

        df["description"] = descriptions

        # validate_output called ONCE here, not in scrape_metadata
        return self.validate_output(df)

    # ------------------------------------------------------------------
    # PHASE 1 — Metadata extraction from listing cards
    # Fast — safe for datacenter IPs (GitHub Actions)
    # ------------------------------------------------------------------

    def scrape_metadata(
        self,
        keywords: list,
        countries: list,
        is_remote: bool = False,
        max_results: int = 100,
    ) -> pd.DataFrame:
        """
        Phase 1: scrape listing cards only — no detail pages visited.
        Designed for fast cloud execution (GitHub Actions, datacenter IPs).
        descriptions are set to None for all records.

        Can be called independently and results stored in DB for
        Phase 2 enrichment later via residential IPs.
        """
        records = []

        for keyword in keywords:
            keyword_slug = keyword.replace(" ", "-")

            for country_code in countries:
                cfg = COUNTRY_CONFIG.get(country_code, {"subdomain": "mx", "currency": "MXN"})
                subdomain = cfg["subdomain"]
                currency = cfg["currency"]
                base_url = f"https://{subdomain}.computrabajo.com"

                page = 1
                while True:
                    if len(records) >= max_results:
                        logger.info("max_results (%s) reached for %s in %s", max_results, keyword, country_code)
                        break

                    if page > MAX_PAGES:
                        logger.info("MAX_PAGES_COMPUTRABAJO (%s) reached for %s in %s", MAX_PAGES, keyword, country_code)
                        break

                    if is_remote:
                        url = f"{base_url}/trabajo-de-{keyword_slug}-modalidad-teletrabajo?p={page}"
                    else:
                        url = f"{base_url}/trabajo-de-{keyword_slug}?p={page}"

                    logger.info("Phase 1 — Fetching page %s: %s", page, url)
                    time.sleep(random.uniform(2, 4))

                    try:
                        resp = self.session.get(url, headers=self._get_headers(), timeout=30)
                        resp.raise_for_status()
                    except requests.RequestException as e:
                        logger.warning("Request failed for %s: %s", url, e)
                        break

                    soup = BeautifulSoup(resp.text, "html.parser")

                    # 1. Ya no buscamos el contenedor padre, buscamos los divs directamente
                    articles = soup.find_all("div", class_="box_offer")
                    if not articles:
                        logger.info("No job cards found — stopping pagination for %s in %s", keyword, country_code)
                        break

                    for article in articles:
                        if len(records) >= max_results:
                            break

                        # Título y URL
                        title_tag = article.select_one("h2 a.js-o-link")
                        title = title_tag.get_text(strip=True) if title_tag else None

                        href = title_tag.get("href") if title_tag else None
                        job_url = f"{base_url}{href}" if (href and href.startswith("/")) else href

                        # Empresa y Ubicación
                        company, raw_location = None, None
                        mt5_paras = article.select("div.fs16 p.mt5")
                        if len(mt5_paras) >= 1:
                            # mt5_paras[0] es el elemento individual, no la lista
                            strings = list(mt5_paras[0].stripped_strings)
                            company = strings[-1] if strings else None
                        if len(mt5_paras) >= 2:
                            # mt5_paras[1] es el segundo elemento
                            raw_location = mt5_paras[1].get_text(strip=True)
                            

                        # Fecha
                        date_tag = article.select_one("p.fc_aux")
                        date_posted = date_tag.get_text(strip=True) if date_tag else None

                        # Salario y Modalidad (Ahora viven juntos en el mismo div)
                        salary_min, salary_max = None, None
                        job_type = "Remoto" if is_remote else None

                        fs12_spans = article.select("div.fs12.mt15 span.dIB")
                        for span in fs12_spans:
                            text = span.get_text(strip=True)
                            if "$" in text:
                                salary_min, salary_max = self._parse_salary(text)
                            elif span.select_one("span.i_home"):
                                job_type = "Remoto"
                            elif span.select_one("span.i_home_office"):
                                job_type = "Híbrido"

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
                            "currency": currency,
                            "job_type": job_type,
                            "country_code": country_code,
                        })

                    next_btn = soup.select_one('span.b_primary.buildLink[data-path]')
                    if not next_btn:
                        logger.info("No more pages for %s in %s", keyword, country_code)
                        break

                    page += 1

        # NOTE: validate_output NOT called here — only called in scrape()
        # so that Phase 1 can be used independently without double-validation
        return pd.DataFrame(records) if records else pd.DataFrame(columns=[
            "site", "job_url", "title", "company", "raw_location",
            "description", "date_posted", "salary_min", "salary_max",
            "currency", "job_type", "country_code"
        ])

    # ------------------------------------------------------------------
    # PHASE 2 — Description enrichment from detail pages
    # Slow — requires residential IPs to avoid detection
    # ------------------------------------------------------------------

    def enrich_descriptions(self, job_urls: list) -> dict:
        """
        Phase 2: visit detail pages to extract full descriptions and
        recover missing salary information.

        Designed for residential IP execution (local machine or
        classmates' home IPs). On 403 or any request error, the URL
        is skipped gracefully — description=None, pipeline never crashes.

        Returns: {job_url: {"description": str | None, "salary_raw": str | None}}
        """
        result = {}

        for url in job_urls:
            logger.info("Phase 2 — Enriching: %s", url)
            time.sleep(random.uniform(3, 6))

            try:
                resp = self.session.get(url, headers=self._get_headers(), timeout=30)
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.warning("Failed to enrich %s: %s", url, e)
                result[url] = {"description": None, "salary_raw": None}
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Computrabajo wraps description in first p.mbB after the h2 header
            desc_tag = soup.select_one('#js_oferta ~ p.mbB') or soup.select_one('p.mbB')
            description = desc_tag.get_text(separator="\n", strip=True) if desc_tag else None

            # Attempt salary recovery from full page text
            salary_raw = None
            page_text = soup.get_text(separator=" ", strip=True)
            salary_match = re.search(
                r'\$\s*[\d,]+\.?\d*(?:\s*[-–]\s*\$?\s*[\d,]+\.?\d*)?',
                page_text,
            )
            if salary_match:
                salary_raw = salary_match.group()
            # "A convenir" means negotiable — leave salary_raw as None

            result[url] = {"description": description, "salary_raw": salary_raw}

        return result

    # ------------------------------------------------------------------
    # INTERNAL HELPERS
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_salary(text: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
        """Parse salary string into (min, max) float tuple. Returns (None, None) if unparseable."""
        if not text:
            return None, None
        numbers = re.findall(r'\$?\s*([\d,]+\.?\d*)', text)
        numbers = [float(n.replace(",", "")) for n in numbers]
        if not numbers:
            return None, None
        if len(numbers) == 1:
            return numbers[0], numbers[0]
        return numbers[0], numbers[1]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    scraper = ComputrabajoScraper()

    print("--- PHASE 1: Fast metadata extraction (simulates GitHub Actions) ---")
    df_phase1 = scraper.scrape_metadata(
        keywords=["data engineer"],
        countries=["MX"],
        is_remote=False,
        max_results=3,
    )
    print(f"Extracted {len(df_phase1)} records. All descriptions are None: {df_phase1['description'].isna().all()}")
    print(df_phase1[["title", "company", "raw_location", "job_type", "country_code"]].to_string())

    print("\n--- PHASE 2: Description enrichment (simulates residential IP) ---")
    urls_to_enrich = df_phase1["job_url"].dropna().tolist()
    enriched_data = scraper.enrich_descriptions(job_urls=urls_to_enrich)

    print(f"Enriched {len(enriched_data)} records.")
    for url, data in enriched_data.items():
        snippet = (data["description"] or "No description found")[:80]
        print(f"  Salary: {data['salary_raw'] or 'N/A'} | Desc: {snippet}...")