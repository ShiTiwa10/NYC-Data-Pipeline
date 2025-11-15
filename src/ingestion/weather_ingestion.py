import os
import logging
import time
import requests
import pandas as pd
import geopandas as gpd
import calendar
from sqlalchemy import create_engine, text
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from dotenv import load_dotenv


load_dotenv()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_PARAMS = [
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "weather_code",
    "relative_humidity_2m",
    "wind_speed_10m",
]
SHAPEFILE_PATH = Path("assets/geospatial/nyc_neighborhoods/nynta2020.shp")


API_HOURLY_LIMIT = 4500  # To handle Open-Meteo's 5000 calls/hour limits
API_CALL_DELAY_SECONDS = 4  # Paces us to 15 calls/min (safely under 600/min cost)


class WeatherIngestor:
    """A self-contained, rate-limited ingestor for fetching historical weather."""

    def __init__(self, year, month, model="ecmwf_ifs"):
        self.year = year
        self.month = month
        self.model = model
        self.db_engine = self._create_db_engine()
        self.all_weather_data = []

        self.num_days_in_month = calendar.monthrange(self.year, self.month)[1]
        self.cost_per_location = self.num_days_in_month  # Assuming 1 day = 1 equivalent

        month_str = f"{self.month:02d}"
        self.table_name = f"weather_hourly_{self.year}_{month_str}"

        target_dir = Path("data/raw/weather")
        target_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = target_dir / f"{self.table_name}.parquet"

    def _create_db_engine(self):
        """Creates and returns a SQLAlchemy engine from .env variables."""
        try:
            db_host = os.getenv("POSTGRES_HOST")
            db_port = os.getenv("POSTGRES_PORT")
            db_name = os.getenv("POSTGRES_DB")
            db_user = os.getenv("POSTGRES_USER")
            db_password = os.getenv("POSTGRES_PASSWORD")
            if not all([db_host, db_port, db_name, db_user, db_password]):
                raise ValueError("Database connection variables not set in .env file.")
            db_connection_str = (
                f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            )
            return create_engine(db_connection_str)
        except Exception as e:
            logging.error(f"Failed to create database engine. Error: {e}")
            return None

    def _table_exists(self, table_name, schema="raw"):
        """Checks if a table exists in the database."""
        query = text(
            f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = '{schema}' AND table_name = '{table_name}'
            );
        """
        )
        try:
            with self.db_engine.connect() as connection:
                return connection.execute(query).scalar()
        except Exception as e:
            logging.error(f"Database check failed: {e}")
            return False

    def _extract_centroids_from_shapefile(self):
        """Reads the NTA shapefile, calculates centroids, and reprojects to WGS 84."""
        logging.info(
            f"Extracting neighborhood centroids from shapefile: {SHAPEFILE_PATH}"
        )
        try:
            gdf = gpd.read_file(SHAPEFILE_PATH)
            centroids_projected = gdf.geometry.centroid
            centroids_wgs84 = centroids_projected.to_crs(epsg=4326)
            gdf["centroid_lat"] = centroids_wgs84.y
            gdf["centroid_lon"] = centroids_wgs84.x
            centroid_list = list(
                zip(gdf["ntaname"], gdf["centroid_lat"], gdf["centroid_lon"])
            )
            logging.info(f"Successfully extracted {len(centroid_list)} centroids.")
            return centroid_list
        except Exception as e:
            logging.error(f"Failed to extract centroids from shapefile. Error: {e}")
            return []

    def _fetch_weather_for_centroid(self, neighborhood, lat, lon, start_date, end_date):
        """Fetches the entire month's hourly weather for a single centroid."""
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": HOURLY_PARAMS,
            "timezone": "America/New_York",
            "models": self.model,
        }
        try:
            response = requests.get(WEATHER_API_URL, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()
            hourly_data = data["hourly"]
            df = pd.DataFrame()
            df["timestamp"] = pd.to_datetime(
                hourly_data["time"], format="%Y-%m-%dT%H:%M"
            )
            for param in HOURLY_PARAMS:
                df[param] = hourly_data[param]
            df["neighborhood_name"] = neighborhood
            return df
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed for {neighborhood}. Error: {e}")
            return None
        except Exception as e:
            logging.error(f"Failed to process data for {neighborhood}. Error: {e}")
            return None

    def _save_to_parquet(self):
        """Saves the accumulated weather data to a Parquet file."""
        if not self.all_weather_data:
            logging.warning("No weather records to save.")
            return False
        logging.info(f"Combining data from {len(self.all_weather_data)} centroids...")
        final_df = pd.concat(self.all_weather_data, ignore_index=True)
        logging.info(
            f"Saving {len(final_df)} total hourly records to {self.output_file}..."
        )
        final_df.to_parquet(
            self.output_file, engine="pyarrow", compression="snappy", index=False
        )
        logging.info("Save complete.")
        return True

    def _load_to_postgres(self):
        """Loads the saved Parquet file into the Postgres raw table."""
        if not self.output_file.exists():
            logging.warning(
                f"Parquet file not found, skipping Postgres load: {self.output_file}"
            )
            return
        try:
            df = pd.read_parquet(self.output_file)
            df.to_sql(
                name=self.table_name,
                con=self.db_engine,
                schema="raw",
                if_exists="replace",
                index=False,
                chunksize=100000,
            )
            logging.info(f"Successfully loaded data into raw.{self.table_name}.")
        except Exception as e:
            logging.error(f"Failed to load data to Postgres. Error: {e}")

    def run(self):
        """Orchestrates the ingestion, respecting API rate limits."""
        logging.info(f"Starting Weather ingestion for {self.year}-{self.month:02d}...")

        if self.db_engine is None:
            logging.error("Database engine not initialized. Aborting.")
            return

        if self._table_exists(self.table_name, schema="raw"):
            logging.info(
                f"Target table raw.{self.table_name} already exists. Skipping."
            )
            return

        centroids = self._extract_centroids_from_shapefile()
        if not centroids:
            logging.error("No centroids found. Aborting.")
            return

        start_date_str = f"{self.year}-{self.month:02d}-01"
        end_date_obj = (
            datetime.strptime(start_date_str, "%Y-%m-%d")
            + relativedelta(months=1)
            - relativedelta(days=1)
        )
        end_date_str = end_date_obj.strftime("%Y-%m-%d")

        current_hour_cost = 0
        total_locations = len(centroids)

        for i, (neighborhood, lat, lon) in enumerate(centroids):

            # RATE LIMIT LOGIC
            if (current_hour_cost + self.cost_per_location) > API_HOURLY_LIMIT:
                logging.warning(
                    f"Hourly API limit ({API_HOURLY_LIMIT}) about to be exceeded. Pausing for 1 hour..."
                )
                time.sleep(3601)  # Rate limit logic in brief in readme
                current_hour_cost = 0

            logging.info(
                f"Fetching data for centroid {i+1}/{total_locations}: {neighborhood}"
            )
            weather_df = self._fetch_weather_for_centroid(
                neighborhood, lat, lon, start_date_str, end_date_str
            )

            if weather_df is not None:
                self.all_weather_data.append(weather_df)
                current_hour_cost += self.cost_per_location

            time.sleep(API_CALL_DELAY_SECONDS)

        if self._save_to_parquet():
            self._load_to_postgres()

        logging.info(f"Weather ingestion complete for {self.year}-{self.month:02d}.")
