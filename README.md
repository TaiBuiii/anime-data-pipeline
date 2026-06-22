# **1. Project Overview**

This project implements an ETL data pipeline that extracts **real-time** anime data from the **Jikan API** (MyAnimeList). The primary goal of this project is to analyze top-rated anime based on various attributes, along with their broadcasting schedules. 

The analytical views in the Gold Layer are designed to drive strategic decisions for production studios, streaming platforms, and investors:
* **Market & Genre Trends (`gold.genre_trend`):** Identifies shifts in audience preferences and market share (%) by genre since 2000, and uncovers niche genres with high fan engagement (`total_favorites`) despite lower production volume.
* **Broadcast Schedule Optimization (`gold.release_timing`):** Discovers the "Prime Time" slots (day/time) that maximize high-tier viewer engagement, helping scheduling teams avoid over-saturated slots and deploy strategic broadcasting.
* **Studio Competency Matrix (`gold.mart_studio_competency`):** Evaluates animation studios based on their core genres and target demographics (`rating_code`), measuring production quality using hit-rates (`high_tier_rate` for scores > 7.5) to select the most reliable production partners.

The data warehouse architecture applied in this project is the **Medallion Architecture**, which divides the system into three logical layers:
* **Bronze Layer:** Stores raw JSON data directly extracted from the Jikan API.
* **Silver Layer:** Transforms, cleans, and structures the data into relational tables.
* **Gold Layer:** Aggregates and transforms data based on specific business and analytical requirements.
<p align="center">
  <img src="architecture.png" alt="Project System Architecture" width="100%">
</p>
# Tech Stack
* **SQL:** Initializing databases, schemas, and DDL definitions.
* **Python:** Transforming data using **Pandas** and connecting to PostgreSQL via **SQLAlchemy**.
* **Docker:** Containerizing the application and database for seamless deployment.

# **2. Project Structure**

```text
anime-data-pipeline/
├── ddl/
│   ├── 01_create_database.sql
│   ├── 02_create_schema.sql
│   ├── 03_init_bronze.sql
│   └── 04_init_silver.sql
├── docker/
│   ├── .dockerignore
│   └── Dockerfile
├── src/
│   ├── ingestion/
│   │   └── jikan_ingestor.py
│   ├── loader/
│   │   ├── base_loader.py
│   │   ├── bronze_loader.py
│   │   └── silver_loader.py
│   ├── logs/
│   ├── orchestrator/
│   │   ├── bronze_orchestrator.py
│   │   ├── gold_orchestrator.py
│   │   └── silver_orchestrator.py
│   ├── transformation/
│   │   ├── gold/
│   │   │   ├── mart_genre_trend.sql
│   │   │   ├── mart_release_timing.sql
│   │   │   └── mart_studio_competency.sql
│   │   └── silver/
│   │       ├── cleaner.py
│   │       ├── extractor.py
│   │       └── normalizer.py
│   ├── utils/
│   │   ├── db.py
│   │   └── logger.py
│   ├── db_init.py
│   └── main.py
├── .env
├── .gitignore
├── docker-compose.yaml
├── LICENSE
├── README.md
└── requirements.txt
```

# **3. Configuration and Setup**

## **3.1 Configuration** 
Create a `.env` file in the root directory and define the following environment variables:

```env
DB_HOST=postgres
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=postgres
```

## **3.2 Setup & Execution**

1. **Start the containers:** Open your terminal in the root folder and run the following command to build and launch the services:
   ```bash
   docker compose up -d --build
   ```

2. **Database Connection:** Use a database management tool like **Navicat** or **DBeaver** to connect to PostgreSQL through the following configuration:
   ```text
   Connection Name: postgres_docker
   Host: localhost (or 127.0.0.1)
   Port: 5433
   Initial Database: postgres
   User Name: postgres
   Password: postgres
   ```

3. **Run the Pipeline:** After successfully connecting to PostgreSQL, execute the project pipeline through:
   ```bash
   docker exec -it anime_data_pipeline_container python src/main.py
