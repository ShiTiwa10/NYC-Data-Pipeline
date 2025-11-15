import os
import logging
import zipfile
import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import ClientError
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
BUCKET_NAME = "tripdata"


class CitibikeIngestor:
    """A self-contained, idempotent ingestor for Citibike monthly trip data."""

    def __init__(self, year, month):
        """Initializes the ingestor for a specific year and month."""
        self.year = year
        self.month = month
        self.s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

        month_str = f"{self.month:02d}"
        self.s3_key = f"{self.year}{month_str}-citibike-tripdata.zip"
        self.target_dir = (
            Path("data/raw/citibike") / f"tripdata-{self.year}-{month_str}"
        )
        self.temp_zip_path = self.target_dir / f"{self.s3_key}.tmp"

        self.target_dir.mkdir(parents=True, exist_ok=True)

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

    def _download_to_temp_file(self):
        """Streams the S3 object to a temporary file on disk."""
        try:
            logging.info(f"Streaming {self.s3_key} to {self.temp_zip_path}...")
            self.s3_client.download_file(
                Bucket=BUCKET_NAME, Key=self.s3_key, Filename=str(self.temp_zip_path)
            )
            logging.info("Download complete.")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logging.warning(f"File not yet available on S3: {self.s3_key}")
            else:
                logging.error(f"An unexpected S3 error occurred: {e}")
            return False

    def _extract_from_zip(self):
        """Extracts CSVs from the downloaded zip file."""
        try:
            logging.info(f"Extracting CSVs to {self.target_dir}...")
            with zipfile.ZipFile(self.temp_zip_path, "r") as zip_ref:
                csv_files = [
                    f
                    for f in zip_ref.namelist()
                    if f.endswith(".csv") and "__MACOSX" not in f
                ]
                zip_ref.extractall(path=self.target_dir, members=csv_files)
            logging.info(f"Successfully extracted {len(csv_files)} files.")
            return [self.target_dir / f for f in csv_files]
        except Exception as e:
            logging.error(f"Failed to extract files: {e}")
            return []

    def _load_to_postgres(self, engine, table_name, csv_files):
        """Loads data from a list of local CSV files into a Postgres table."""
        if not csv_files:
            logging.warning("No CSV files found to load into Postgres.")
            return

        logging.info(f"Preparing to load data into raw.{table_name}...")
        try:
            for i, file_path in enumerate(csv_files):
                chunk_iterator = pd.read_csv(
                    file_path, chunksize=100000, low_memory=False
                )
                for j, chunk in enumerate(chunk_iterator):
                    if_exists_strategy = "replace" if i == 0 and j == 0 else "append"
                    chunk.to_sql(
                        name=table_name,
                        con=engine,
                        schema="raw",
                        if_exists=if_exists_strategy,
                        index=False,
                    )
            logging.info(f"Successfully loaded all data into raw.{table_name}.")
        except Exception as e:
            logging.error(f"Failed to load data to Postgres. Error: {e}")

    def _cleanup(self):
        """Removes the temporary zip file."""
        if os.path.exists(self.temp_zip_path):
            logging.info(f"Cleaning up temporary file: {self.temp_zip_path}")
            os.remove(self.temp_zip_path)

    def run(self):
        """The main public method, now fully idempotent by checking the target database first."""
        logging.info(f"Starting Citibike ingestion for {self.year}-{self.month:02d}...")

        try:
            # Set up the database connection
            db_host = os.getenv("POSTGRES_HOST")
            db_port = os.getenv("POSTGRES_PORT")
            db_name = os.getenv("POSTGRES_DB")
            db_user = os.getenv("POSTGRES_USER")
            db_password = os.getenv("POSTGRES_PASSWORD")
            db_connection_str = (
                f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            )
            engine = create_engine(db_connection_str)

            table_name = f"citibike_trips_{self.year}_{self.month:02d}"

            if self._table_exists(engine, table_name, schema="raw"):
                logging.info(f"Target table raw.{table_name} already exists. Skipping.")
                return

        except Exception as e:
            logging.error(f"Failed during database pre-check. Error: {e}")
            return

        extracted_files = []
        try:
            if not self._download_to_temp_file():
                return

            extracted_files = self._extract_from_zip()

            if extracted_files:
                self._load_to_postgres(engine, table_name, extracted_files)
        finally:
            self._cleanup()

        logging.info(f"Citibike EL process complete for {self.year}-{self.month:02d}.")
