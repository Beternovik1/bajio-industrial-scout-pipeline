from abc import ABC, abstractmethod
import pandas as pd

REQUIRED_COLUMNS = [
    "site", "job_url", "title", "company",
    "raw_location", "description", "date_posted",
    "salary_min", "salary_max", "currency", "job_type",
    "country_code"
]


class BaseScraper(ABC):

    @abstractmethod
    def scrape(self, keywords: list, countries: list, is_remote: bool = False, max_results: int = 100) -> pd.DataFrame:
        pass

    def validate_output(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = None
        return df[REQUIRED_COLUMNS]
