# Urban Mobility Context Engine (NYC)

![Status](https://img.shields.io/badge/Status-🚧_In_Progress-yellow)

This project is an end-to-end data engineering pipeline designed to build a "Context Engine" for analyzing Citibike ridership in New York City. The pipeline ingests data from disparate sources, models it, and creates a final, analytics-ready data product that explains the "why" behind daily and hourly mobility patterns.

**Status Update (Nov 15, 2025):** The core architecture, containerized environment, and all ingestion scripts are complete. The full data backfill for Q3 2025 is scheduled to run from November 16-19, 2025.

---

## Tech Stack & Architecture

This pipeline is built on a modern, local-first ELT framework using a **Medallion Architecture**.

| Category | Technology |
| :--- | :--- |
| **Orchestration** | **Apache Airflow** |
| **Containerization** | **Docker & Docker Compose** |
| **Database** | **PostgreSQL + PostGIS/H3** |
| **Transformation** | **dbt Core** |
| **Ingestion** | **Python (OOP, Pandas, GeoPandas)** |
| **Presentation** | **Streamlit** (from a static Parquet export) |

### The Data Flow (ELT)

1.  **Extract & Load (EL):** Python ingestor classes, orchestrated by Airflow, fetch data from various sources and load it into a **Bronze** `raw` schema in Postgres.
2.  **Transform (T):** `dbt` runs all transformations, moving data from:
    * **Silver 🥈:** `staging` models that clean, cast, and test the raw data.
    * **Gold 🥇:** Final, denormalized `marts` tables (a Star Schema) that join all context layers, ready for analysis.

---

## Data Sources

| Context Layer | Data Source | Ingestion Method |
| :--- | :--- | :--- |
| **Core Activity** | Citi Bike Trip Data | Python (Boto3 from S3) |
| **Weather** | Open-Meteo Archive | Python (API per neighborhood centroid) |
| **Events** | NYC Permitted Events | Python (Generic Socrata API Ingestor) |
| **Infrastructure** | NYC 311 Service Requests | Python (Generic Socrata API Ingestor) |
| **Temporal** | NYC Holidays | `dbt seed` (from CSV) |
| **Geospatial** | NYC Neighborhoods (NTA) | Manual `shp2pgsql` load (one-time setup) |

---

## Project Roadmap

* [x] **Phase 1: Architecture & Setup**
    * [x] Design ELT pipeline and Medallion architecture.
    * [x] Configure containerized environment with Docker Compose.
    * [x] Build all ingestion scripts (Citibike, Socrata, Weather) as idempotent Python classes.
    * [x] Set up dbt project and connect to Postgres.
* [ ] **Phase 2: Data Backfill (In Progress)**
    * [ ] Run one-time setup for static data (Holidays, Shapefiles).
    * [ ] Execute full data backfill for **Q3 2025 (July, Aug, Sep)**.
* [ ] **Phase 3: Transformation**
    * [ ] Build all Silver (staging) dbt models.
    * [ ] Build all Gold (mart) dbt models, including the final `fct_trip_hourly` table.
* [ ] **Phase 4: Showcase**
    * [ ] Export the final Gold data to a static Parquet file.
    * [ ] Build and deploy a public-facing Streamlit dashboard.

## How to Run (Development)

1.  Clone this repository.
2.  Create a `.env` file (see `.env.example` for required variables).
3.  Run `docker-compose up --build` to launch the Airflow and Postgres services.
4.  *(Detailed setup and run instructions to be added)*