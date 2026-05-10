# Tokyo Air Quality Pipeline

Tokyo Air Quality Pipeline is a Docker-containerized data pipeline that extracts daily air pollutant measurements from Tokyo sensors via [OpenAQ](https://openaq.org) public archive, then transforms the raw data using the Polars library and loads it into a PostgreSQL database for permanency and an Elasticsearch index for Kibana visualizations.

This project was built for the course YZV322E Applied Data Engineering @ Istanbul Technical University, Spring 2026.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Apache Airflow DAG                          │
│          openaq_tokyo_daily  (catchup, April 2026)              │
│                                                                 │
│   fetch_locations ──► fetch_measurements ──► transform_load     │
└────────┬──────────────────┬───────────────────────┬────────────┘
         │                  │                       │
         ▼                  ▼                       ▼
   OpenAQ v3 API     OpenAQ S3 Archive        raw_data/*.json
   (bbox Tokyo)      (CSV.gz per location     (stage handoff
   locations_*.json   per day, parallel)       via files)
                                                    │
                    ┌───────────────────────────────┘
                    ▼
              Polars transform
         (normalize / deduplicate)
                    │
          ┌─────────┴──────────┐
          ▼                    ▼
     PostgreSQL 16        Elasticsearch 8
     ┌────────────┐       ┌──────────────────────┐
     │ locations  │       │ aq-measurements       │
     │ sensors    │       │ aq-daily-summaries    │
     │ measurements│      └──────────────────────┘
     │daily_summ. │                │
     └────────────┘                ▼
          │                     Kibana 8
          ▼                  (dashboards at
       pgAdmin               localhost:5601)
   (localhost:5050)
```

**Data flow summary:** Airflow schedules one DAG run per all days of April 2026. Each run fetches location data regarding suitable sensors from the OpenAQ API, downloads the corresponding day's CSV archives from S3 in parallel(this is not done on the API due to rate limits). The raw data is then normalized with Polars, inserted into or updated in Postgres, and bulk-indexed into Elasticsearch for interactive Kibana dashboards. Pipeline stages communicate via timestamped JSON files in `raw_data/`, as opposed to XCom, so each stage can be re-run independently.

---

## Tools Used

| Tool | Role |
|---|---|
| **Apache Airflow 2.10.5** | Used for scheduling tasks, creating and running them as DAGs and "catching up" with the tasks. |
| **PostgreSQL 16** | Relational storage database for cleaned and summarized information, used for persistence and would allow further analysis if needed. |
| **pgAdmin 4** | Database administration system to quickly and easily monitor the PostgreSQL database.|
| **Elasticsearch 8.15** | Record indexing, primarily to integrate data into Kibana. |
| **Kibana 8.15** | Interactive dashboards and visualizations. |
| **Polars 1.35** | Fast columnar transformation in Python, picked in place of Pandas due to size of the data at hand. |

---

## Quick Start

**Prerequisites:** Docker and Docker Compose. As indicated on the requirements, no sort of installations are necessary on the host machine.

```bash
# 1. Clone the repository
git clone <repo-url>
cd YZV322E-Project

# 2. Set up environment and add your OpenAQ API Key (see https://docs.openaq.org/using-the-api/api-key for details)
cp .env.example .env
# edit .env and set OPENAQ_API_KEY=<your key>

# 3. Start the full stack
docker compose up --build
```

The pipeline starts automatically. Airflow backfills all 30 days of April 2026 as `catchup=True` is set. Depending on network speed, downloading and processing the entirety of the April data may take between 10 to 20 minutes. Because demo does not have this much time, our showcase will be conducted on data we have access to.

### Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| pgAdmin | http://localhost:5050 | see `.env` |
| Kibana | http://localhost:5601 | no auth |
| Elasticsearch | http://localhost:9200 | no auth |
| Postgres | http://localhost:5432 | see `.env` |

### Stop and Reset

```bash
docker compose down          # stops the pipeline, but bind mounts and volumes keep the persistent data.
docker compose down -v       # stops and wipes all data (full clean restart)
```

---

## Example Commands

You can run individual pipeline stages manually inside the scheduler container:

```bash
# Fetching location data
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow && python scripts/fetch_locations.py"

# Fetch measurements for a specific date
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow && TARGET_DATE=2026-04-15 python scripts/fetch_measurements.py"

# Transform and load a specific date
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow && TARGET_DATE=2026-04-15 python scripts/transform_load.py"

# Trigger a DAG run manually
docker compose exec airflow-scheduler \
  airflow dags trigger openaq_tokyo_daily -e 2026-04-15
```

For re-generating Kibana saved objects after editing, `kibana/build_saved_objects.py`:

```bash
python kibana/build_saved_objects.py
```

---

## Repository Structure

```
.
├── dags/                   # Airflow DAG definition
├── scripts/                # ETL scripts (fetch_locations, fetch_measurements, transform_load, common)
├── sql/                    # PostgreSQL schema (auto-loaded on first volume init)
├── kibana/                 # Kibana saved objects and init script
├── pgadmin/                # pgAdmin server config
├── raw_data/               # Stage handoff JSON files (gitignored except .gitkeep)
├── clean_data/             # Transformed CSVs (gitignored except .gitkeep)
├── docker-compose.yml
├── Dockerfile.airflow
├── requirements.txt
└── .env.example
```

---

## Known Limitations

- **April 2026 only:** The DAG schedule and `end_date` are hardcoded to April 2026. No data outside this window is fetched or processed. This has been clearly communicated in the abstract.
- **Requires internet on first run:** Location metadata and S3 archive files are downloaded live; there is no offline mode. However, as the committing of large files is disliked, we do not have any other solutions to this.
- **No real-time ingestion:** The pipeline is batch-only (one run per day). OpenAQ sensor data would be fetched from the S3 archive with roughly 24-hour latency at best. Daily delay is a 72 hour delay in practice, as the data bucket we are using has a 3-day delay for writing records.
- **Elasticsearch security disabled:** `xpack.security.enabled=false` for local development simplicity. Not suitable for production. This is the standard usage we have seen in course, and therefore disabling security was decided on, due to ease of use. For the same reason, our Elasticsearch runs on a single node as well.
- **S3 archive gaps:** Some location-days return HTTP 404 (no data uploaded by the sensor). These are counted as `missing` and skipped cleanly.

---

## Team

| Name | Student ID |
|---|---|
| Burak Emre Polat | 150230328 |
| Hasan Yalçın Arıkanoğlu | 150220341 |
| Metin Furkan Amarat | 150230301 |

---
*Note: Due to technical reasons outside our control(mainly WSL usage and 2FA complications), commits by Metin Furkan Amarat are shared across two GitHub accounts. I hereby declare and confirm that both "amaratmetin" and "mmmm-tr" accounts are mine and their commits belong to my single person.*

*YZV322E — Applied Data Engineering · Istanbul Technical University · Spring 2026*
