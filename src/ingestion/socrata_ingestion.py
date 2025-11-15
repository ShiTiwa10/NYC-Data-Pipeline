import os
import logging
import pandas as pd
import requests
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class SocrataIngestor:
    """A generic, config-driven ingestor for Socrata datasets."""

    def __init__(self, config, year, month):
        """Initializes the ingestor with a specific dataset config, year, and month."""
        self.config = config
        self.year = year
        self.month = month
        self.app_token = os.getenv("NYC_OPEN_DATA_APP_TOKEN")
        self.all_records = []

        month_str = f"{self.month:02d}"
        target_dir = Path(self.config["output_dir"])
        target_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = (
            target_dir
            / f"{self.config['filename_prefix']}-{self.year}-{month_str}.parquet"
        )

    def _table_exists(self, engine, table_name, schema="raw"):
        """Checks if a table exists in a given schema."""
        query = text(
            f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name}'
            );
        """
        )
        with engine.connect() as connection:
            return connection.execute(query).scalar()

    def _fetch_data_chunk(self, params):
        """Executes a single, generic GET request to a Socrata endpoint."""
        headers = {"X-App-Token": self.app_token}
        endpoint = (
            f"https://data.cityofnewyork.us/resource/{self.config['dataset_id']}.json"
        )

        try:
            response = requests.get(
                endpoint, headers=headers, params=params, timeout=120
            )
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"API request to {endpoint} failed: {e}")
            return None
        except json.JSONDecodeError:
            logging.error(f"Failed to parse JSON response from {endpoint}.")
            return None

    def _fetch_all_pages(self):
        """Orchestrates the paginated fetching for the dataset."""
        start_date = datetime(self.year, self.month, 1)
        end_date = start_date + relativedelta(months=1)
        start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S")

        page_size = 50000
        offset = 0

        while True:
            # SoQL (Socrata Query Language) filter
            params = {
                "$where": f"{self.config['date_column']} >= '{start_str}' AND {self.config['date_column']} < '{end_str}'",
                "$limit": page_size,
                "$offset": offset,
                "$order": self.config["date_column"],
            }

            data_chunk = self._fetch_data_chunk(params)

            if data_chunk is None:
                return False

            if not data_chunk:
                logging.info("No more records to fetch. Pagination complete.")
                break

            self.all_records.extend(data_chunk)
            logging.info(
                f"Fetched {len(data_chunk)} records. Total so far: {len(self.all_records)}"
            )

            if len(data_chunk) < page_size:
                break

            offset += page_size

        return True

    def _save_to_parquet(self):
        """Saves the fetched records to a Parquet file."""
        if not self.all_records:
            logging.warning(f"No records to save for {self.config['filename_prefix']}.")
            return

        logging.info(f"Saving {len(self.all_records)} records to {self.output_file}...")
        df = pd.DataFrame(self.all_records)
        df.to_parquet(
            self.output_file, engine="pyarrow", compression="snappy", index=False
        )
        logging.info("Save complete.")

    def _load_to_postgres(self, engine, table_name):
        """Loads the saved Parquet file into the Postgres raw table."""
        if not self.output_file.exists():
            logging.warning(
                f"Parquet file not found, skipping Postgres load: {self.output_file}"
            )
            return

        try:
            df = pd.read_parquet(self.output_file)
            df.to_sql(
                name=table_name,
                con=engine,
                schema="raw",
                if_exists="replace",
                index=False,
                chunksize=100000,
            )
            logging.info(f"Successfully loaded data into raw.{table_name}.")
        except Exception as e:
            logging.error(f"Failed to load data to Postgres. Error: {e}")
            # Optionally, you could try to clean up the partially loaded table here

        except Exception as e:
            logging.error(f"Failed to load data to Postgres. Error: {e}")
            return False

    def run(self):
        """
        The main public method. It is now truly idempotent by checking the target database first.
        """
        logging.info(
            f"Starting ingestion for '{self.config['filename_prefix']}' for {self.year}-{self.month:02d}..."
        )

        try:
            db_host = os.getenv("POSTGRES_HOST")
            db_port = os.getenv("POSTGRES_PORT")
            db_name = os.getenv("POSTGRES_DB")
            db_user = os.getenv("POSTGRES_USER")
            db_password = os.getenv("POSTGRES_PASSWORD")
            db_connection_str = (
                f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            )
            engine = create_engine(db_connection_str)

            table_name = (
                f"{self.config['filename_prefix']}_{self.year}_{self.month:02d}"
            )

            if self._table_exists(engine, table_name, schema="raw"):
                logging.info(f"Target table raw.{table_name} already exists. Skipping.")
                return

        except Exception as e:
            logging.error(f"Failed during database pre-check. Error: {e}")
            return

        logging.info(
            f"Target table raw.{table_name} does not exist. Starting full EL process."
        )

        if self._fetch_all_pages():
            self._save_to_parquet()
            self._load_to_postgres(engine, table_name)

        logging.info(f"Ingestion complete for '{self.config['filename_prefix']}'.")
