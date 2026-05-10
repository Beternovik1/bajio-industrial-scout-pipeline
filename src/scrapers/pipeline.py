#src/scrapers/pipeline.py
import logging
import os
import time
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from database.models import db_connect, create_tables, Job
from etl.extract import extract_data
from etl.transform import transform_data, clean, detect_state
from etl.loaders import load_data
from shared.config import NEW_SCRAPERS_CONFIG, ENRICHMENT_CONFIG
from scrapers.computrabajo_scraper import ComputrabajoScraper
from scrapers.occ_scraper import OCCScraper
from shared.config import include_linkedin_today

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def _map_phase1_records(df, search_term, industry_niche):
    """Map DataFrame from Phase 1 scraper to Job model dict list."""
    worker_name = os.getenv("WORKER_NAME", "unknown")
    records = []
    # Agrega este mapeo antes de hacer el append
    JOB_TYPE_MAP = {
        "Remoto": "remoto",
        "Híbrido": "hibrido", 
        "Presencial": "presencial",
        "Remote": "remoto",
        "Hybrid": "hibrido",
    }
    for _, row in df.iterrows():
        raw_location = clean(row.get("raw_location"), "Ubicación desconocida")
        records.append({
            "site": row.get("site"),
            "job_url": row.get("job_url"),
            "title": str(clean(row.get("title"), "")).upper(),
            "company": clean(row.get("company"), "Empresa confidencial"),
            "raw_location": raw_location,
            "country": "México",
            "country_code": row.get("country_code", "MX"),
            "state": detect_state(raw_location),
            "career": search_term,
            "scraped_by": worker_name,
            "industry_niche": industry_niche,
            "job_type": JOB_TYPE_MAP.get(row.get("job_type"), row.get("job_type")),            
            "salary_min": row.get("salary_min"),
            "salary_max": row.get("salary_max"),
            "currency": clean(row.get("currency"), "MXN"),
            "description": None,
            "date_posted": row.get("date_posted"),
            "status": "PENDING_ENRICHMENT",
        })
    return records


def run_pipeline(search_config, results_limit, include_linkedin=False):
    engine = db_connect()
    create_tables(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        for search_term, location, industry_niche in search_config:
            logger.info(f"==> Extrayendo: {search_term} en {location} (Nicho: {industry_niche})")

            raw_df = extract_data(
                search_term=search_term,
                location=location,
                results_limit=results_limit,
                include_linkedin=include_linkedin
            )

            if raw_df.empty:
                logger.warning(f"No hay datos para: {search_term}. Saltando...")
                continue

            records = transform_data(raw_df, search_term, industry_niche)
            load_data(session, records)

    except Exception as error:
        logger.error(f"(PIPELINE) Falló: {error}")
        raise
    finally:
        session.close()
        logger.info("Conexión de base de datos cerrada.")


def run_cloud_extraction_pipeline(search_config, results_limit):
    """
    Runs on GitHub Actions (datacenter IPs).
    - Runs JobSpy (Indeed only, exclude LinkedIn).
    - Checks `enabled` flag in NEW_SCRAPERS_CONFIG before running Computrabajo/OCC Phase 1.
    - Keywords extracted dynamically from search_config.
    - Inserts all results into the DB (Phase 1 records get status='PENDING_ENRICHMENT').
    """
    engine = db_connect()
    create_tables(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    total_inserted = {"jobspy": 0, "computrabajo": 0, "occ": 0}
    total_skipped = {"jobspy": 0, "computrabajo": 0, "occ": 0}

    computrabajo_enabled = NEW_SCRAPERS_CONFIG["computrabajo"]["enabled"]
    occ_enabled = NEW_SCRAPERS_CONFIG["occ"]["enabled"]

    computrabajo_scraper = ComputrabajoScraper() if computrabajo_enabled else None
    occ_scraper = OCCScraper() if occ_enabled else None

    try:
        for search_term, location, industry_niche in search_config:
            logger.info("==> Cloud extraction: %s en %s (Niche: %s)", search_term, location, industry_niche)

            raw_df = extract_data(
                search_term=search_term,
                location=location,
                results_limit=results_limit,
                include_linkedin=include_linkedin_today(),
            )

            if not raw_df.empty:
                records = transform_data(raw_df, search_term, industry_niche)
                inserted, skipped = load_data(session, records)
                total_inserted["jobspy"] += inserted
                total_skipped["jobspy"] += skipped
            else:
                logger.warning("No JobSpy data for: %s", search_term)

            if computrabajo_scraper:
                logger.info("Computrabajo Phase 1 for: %s", search_term)
                df_ct = computrabajo_scraper.scrape_metadata(
                    keywords=[search_term],
                    countries=NEW_SCRAPERS_CONFIG["computrabajo"]["countries"],
                    is_remote=NEW_SCRAPERS_CONFIG["computrabajo"]["is_remote"],
                    max_results=NEW_SCRAPERS_CONFIG["computrabajo"]["max_results"],
                )
                if not df_ct.empty:
                    records = _map_phase1_records(df_ct, search_term, industry_niche)
                    inserted, skipped = load_data(session, records)
                    total_inserted["computrabajo"] += inserted
                    total_skipped["computrabajo"] += skipped

            if occ_scraper:
                logger.info("OCC Phase 1 for: %s", search_term)
                df_occ = occ_scraper.scrape_metadata(
                    keywords=[search_term],
                    countries=["MX"],
                    is_remote=NEW_SCRAPERS_CONFIG["occ"]["is_remote"],
                    max_results=NEW_SCRAPERS_CONFIG["occ"]["max_results"],
                )
                if not df_occ.empty:
                    records = _map_phase1_records(df_occ, search_term, industry_niche)
                    inserted, skipped = load_data(session, records)
                    total_inserted["occ"] += inserted
                    total_skipped["occ"] += skipped

    except Exception as error:
        logger.error("(CLOUD EXTRACTION) Failed: %s", error)
        raise
    finally:
        session.close()
        logger.info(
            "Cloud extraction summary — JobSpy: %d inserted, %d skipped | "
            "Computrabajo: %d inserted, %d skipped | "
            "OCC: %d inserted, %d skipped",
            total_inserted["jobspy"], total_skipped["jobspy"],
            total_inserted["computrabajo"], total_skipped["computrabajo"],
            total_inserted["occ"], total_skipped["occ"],
        )


def run_local_enrichment_pipeline():
    """
    Runs locally (residential IPs).
    1. Queries the database using the existing ORM session for ALL records
       WHERE site IN ('computrabajo','occ') AND status='PENDING_ENRICHMENT'.
    2. Groups URLs by site.
    3. ANTI-BAN LOGIC (using ENRICHMENT_CONFIG):
       - Cap the total records processed per run using `daily_limit`.
       - Process the URLs in chunks based on `batch_size`.
       - Apply a time.sleep(`cooldown_seconds`) between chunks.
       - CIRCUIT BREAKER: If there are `circuit_breaker_threshold` consecutive
         errors from the SAME site, sleep for `circuit_breaker_sleep` seconds
         before continuing.
    4. Calls .enrich_descriptions() for the respective scraper.
    5. Updates the database using the ORM.
    """
    engine = db_connect()
    Session = sessionmaker(bind=engine)
    session = Session()

    config = ENRICHMENT_CONFIG
    daily_limit = config["daily_limit"]
    batch_size = config["batch_size"]
    cooldown = config["cooldown_seconds"]
    circuit_threshold = config["circuit_breaker_threshold"]
    circuit_sleep = config["circuit_breaker_sleep"]

    try:
        pending = session.query(Job).filter(
            Job.site.in_(["computrabajo", "occ"]),
            Job.status == "PENDING_ENRICHMENT",
        ).all()

        if not pending:
            logger.info("No PENDING_ENRICHMENT records found. Nothing to do.")
            return

        logger.info("Found %d records pending enrichment", len(pending))

        urls_by_site = {}
        for job in pending:
            urls_by_site.setdefault(job.site, []).append(job.job_url)

        url_to_job = {job.job_url: job for job in pending}

        total_enriched = 0
        total_orphaned = 0
        total_processed = 0

        for site in ["computrabajo", "occ"]:
            if site not in urls_by_site:
                continue

            urls = urls_by_site[site]
            remaining = daily_limit - total_processed
            if remaining <= 0:
                logger.info("Daily enrichment limit (%d) reached", daily_limit)
                break

            urls = urls[:remaining]

            if site == "computrabajo":
                scraper = ComputrabajoScraper()
            else:
                scraper = OCCScraper()

            consecutive_errors = 0

            for i in range(0, len(urls), batch_size):
                batch = urls[i:i + batch_size]
                logger.info(
                    "Enriching %s batch %d/%d (%d URLs)",
                    site,
                    i // batch_size + 1,
                    (len(urls) + batch_size - 1) // batch_size,
                    len(batch),
                )

                enriched = scraper.enrich_descriptions(batch)

                if not enriched:
                    logger.warning("Empty enrich result for %s batch, skipping", site)
                    total_processed += len(batch)
                    session.commit()
                    if i + batch_size < len(urls):
                        time.sleep(cooldown)
                    continue

                failed_urls = sum(
                    1 for data in enriched.values()
                    if data.get("description") is None
                )

                for url, data in enriched.items():
                    job = url_to_job.get(url)
                    if not job:
                        continue

                    description = data.get("description")

                    if description is not None:
                        job.description = description
                        salary_raw = data.get("salary_raw")
                        if salary_raw:
                            parsed_min, parsed_max = scraper._parse_salary(salary_raw)
                            if job.salary_min is None and parsed_min is not None:
                                job.salary_min = parsed_min
                                job.salary_max = parsed_max
                        job.status = "NEW"
                        total_enriched += 1
                    else:
                        job.status = "ORPHANED"
                        total_orphaned += 1

                    job.last_updated = func.now()

                if failed_urls == len(enriched):
                    consecutive_errors += 1
                    logger.warning(
                        "Circuit breaker: %d/%d consecutive all-fail batches for %s",
                        consecutive_errors, circuit_threshold, site,
                    )
                    if consecutive_errors >= circuit_threshold:
                        logger.warning(
                            "Circuit breaker tripped for %s. Sleeping %d seconds",
                            site, circuit_sleep,
                        )
                        time.sleep(circuit_sleep)
                        consecutive_errors = 0
                else:
                    consecutive_errors = 0

                total_processed += len(batch)
                session.commit()

                if i + batch_size < len(urls):
                    logger.info("Cooldown %d seconds between batches", cooldown)
                    time.sleep(cooldown)

        remaining_pending = session.query(Job).filter(
            Job.site.in_(["computrabajo", "occ"]),
            Job.status == "PENDING_ENRICHMENT",
        ).count()

        logger.info(
            "Enrichment summary — Total enriched: %d, ORPHANED: %d, Remaining PENDING_ENRICHMENT: %d",
            total_enriched, total_orphaned, remaining_pending,
        )

    except Exception as error:
        logger.error("(LOCAL ENRICHMENT) Failed: %s", error)
        raise
    finally:
        session.close()
