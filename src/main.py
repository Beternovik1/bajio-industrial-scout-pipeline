# src/main.py
import os
from scrapers.pipeline import run_pipeline, run_cloud_extraction_pipeline, run_local_enrichment_pipeline
from reporting_legacy.csv_export import export_jobs
from shared.config import get_search_config, SCRAPER_CONFIG, include_linkedin_today


def main():
    mode = os.getenv("EXECUTION_MODE", "local_full")
    search_config = get_search_config()
    results_limit = SCRAPER_CONFIG["results_limit"]

    if mode == "cloud_extraction":
        print("MODE: Cloud Extraction (Phase 1 via Datacenter)")
        run_cloud_extraction_pipeline(search_config, results_limit)

    elif mode == "local_enrichment_only":
        print("MODE: Local Enrichment Only (Phase 2 via Residential IP)")
        run_local_enrichment_pipeline()

    elif mode == "local_full":
        print("MODE: Local Full (JobSpy + Phase 2 Enrichment)")
        include_linkedin = include_linkedin_today()
        run_pipeline(search_config, results_limit, include_linkedin)
        run_local_enrichment_pipeline()

    else:
        print(f"Unknown EXECUTION_MODE: {mode}")
        return

    export_jobs()
    print("Pipeline completed successfully!")


if __name__ == "__main__":
    main()
