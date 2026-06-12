FROM apache/airflow:3.2.2-python3.12

# Switch to root to install system dependencies for PostGIS/GeoPandas
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        gdal-bin \
        libgdal-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user to install Python packages
USER airflow

# Copy requirements and install
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt