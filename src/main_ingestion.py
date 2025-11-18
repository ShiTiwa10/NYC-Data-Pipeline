import argparse
import logging
from ingestion.citibike_ingestion import CitibikeIngestor
from ingestion.socrata_ingestion import SocrataIngestor
from ingestion.weather_ingestion import WeatherIngestor
from ingestion.config import SOCRATA_DATASETS


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def run_all_ingestions(year, month):
    """
    The main orchestration function. It creates and runs an ingestor
    for each data source.
    """
    logging.info(f"===== STARTING INGESTION PIPELINE FOR {year}-{month:02d} =====")

    ingestion_jobs = [
        # The Citibike ingestor is called directly
        CitibikeIngestor(year=year, month=month),
        WeatherIngestor(year=year, month=month, model="ecmwf_ifs"),
    ]
    # Add Socrata ingestors based on config
    for config in SOCRATA_DATASETS:
        ingestion_jobs.append(SocrataIngestor(config, year, month))

    success_count = 0
    total_jobs = len(ingestion_jobs)

    for i, ingestor in enumerate(ingestion_jobs):
        job_name = ingestor.__class__.__name__
        if hasattr(ingestor, "config"):
            job_name = ingestor.config.get("name", job_name)

        logging.info(f"--- Starting job {i+1}/{total_jobs}: {job_name} ---")
        try:
            ingestor.run()
            success_count += 1
            logging.info(f"--- Finished job: {job_name} ---")

        except Exception as e:
            logging.error(f"--- FAILED job: {job_name}. Error: {e} ---")

    logging.info(
        f"===== INGESTION PIPELINE COMPLETE. Successfully ran {success_count}/{total_jobs} ingestors. ====="
    )


if __name__ == "__main__":
    # To recieve year and month from command line arguments or Airflow parameters
    parser = argparse.ArgumentParser(
        description="Run the main monthly ingestion pipeline for all data sources."
    )
    parser.add_argument(
        "--year", required=True, type=int, help="The year to process (e.g., 2025)"
    )
    parser.add_argument(
        "--month",
        required=True,
        type=int,
        help="The month to process (e.g., 7 for July)",
    )

    args = parser.parse_args()

    run_all_ingestions(args.year, args.month)
