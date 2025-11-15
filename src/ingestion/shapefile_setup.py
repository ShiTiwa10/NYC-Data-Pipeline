import os
import logging
import geopandas as gpd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_shapefile_to_postgis():
    """
    Reads the NYC Neighborhoods shapefile and loads it into a PostGIS table.
    """
    shapefile_path = "assets/geospatial/nyc_neighborhoods_2025/nynta2020.shp"
    table_name = "nyc_neighborhoods"
    schema_name = "raw"

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
        engine = create_engine(db_connection_str)

        logging.info(f"Reading shapefile from: {shapefile_path}")
        gdf = gpd.read_file(shapefile_path)
        logging.info(f"Found {len(gdf)} shapes to load.")

        logging.info(f"Loading data into {schema_name}.{table_name}...")
        gdf.to_postgis(
            name=table_name,
            con=engine,
            schema=schema_name,
            if_exists="replace",
            index=False,
        )
        logging.info("Successfully loaded shapefile into PostGIS.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    load_shapefile_to_postgis()
