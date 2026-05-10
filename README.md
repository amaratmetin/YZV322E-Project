# Tokyo Air Quality Pipeline

End-to-end containerized data engineering pipeline that ingests daily air quality measurements from Tokyo sensors via the [OpenAQ](https://openaq.org) public archive, transforms and loads them into PostgreSQL, and indexes them into Elasticsearch for interactive Kibana dashboards.

Built for **YZV322E — Applied Data Engineering**, Istanbul Technical University, Spring 2026.

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

**Data flow summary:** Airflow schedules one DAG run per day for April 2026. Each run fetches location metadata from the OpenAQ API, downloads the corresponding day's CSV archives from S3 in parallel, normalizes the data with Polars, upserts into Postgres, and bulk-indexes into Elasticsearch. Pipeline stages communicate via timestamped JSON files in `raw_data/` — not XCom — so each stage can be re-run independently.

---

## Tools Used

| Tool | Role |
|---|---|
| **Apache Airflow 2.10.5** | DAG orchestration, scheduling, catchup backfill |
| **PostgreSQL 16** | Relational storage — source of truth |
| **pgAdmin 4** | Database administration UI |
| **Elasticsearch 8.15** | Document indexing for analytics queries |
| **Kibana 8.15** | Interactive dashboards and visualizations |
| **Polars 1.35** | Fast columnar transformation in Python |

---

## Quick Start

**Prerequisites:** Docker and Docker Compose. No Python, pip, or any other runtime needed on the host machine.

```bash
# 1. Clone the repository
git clone <repo-url>
cd YZV322E-Project

# 2. Set up environment (add your OpenAQ API key)
cp .env.example .env
# edit .env and set OPENAQ_API_KEY=<your key>

# 3. Start the full stack
docker compose up --build
```

The pipeline starts automatically. Airflow backfills all 30 days of April 2026 via `catchup=True`. Full backfill takes approximately 10–20 minutes depending on network speed.

### Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| pgAdmin | http://localhost:5050 | see `.env` |
| Kibana | http://localhost:5601 | no auth |
| Elasticsearch | http://localhost:9200 | no auth |
| Postgres | localhost:**5433** | see `.env` |

### Stop and Reset

```bash
docker compose down          # stop (data persists in named volumes)
docker compose down -v       # stop AND wipe all data (full clean restart)
```

---

## Example Commands

Run individual pipeline stages manually inside the scheduler container:

```bash
# Fetch location metadata
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

Re-generate Kibana saved objects after editing `kibana/build_saved_objects.py`:

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

- **April 2026 only:** The DAG schedule (`0 0 1-30 4 *`) and `end_date` are hardcoded to April 2026. No data outside this window is fetched or processed.
- **Requires internet on first run:** Location metadata and S3 archive files are downloaded live; there is no offline mode.
- **No real-time ingestion:** The pipeline is batch-only (one run per day). OpenAQ sensor data is fetched from the S3 archive with roughly 24-hour latency.
- **Elasticsearch security disabled:** `xpack.security.enabled=false` for local development simplicity. Not suitable for production.
- **Single-node Elasticsearch:** Configured with 1 shard and 0 replicas (`ES_JAVA_OPTS: -Xms512m -Xmx512m`). Designed for a laptop, not a production cluster.
- **S3 archive gaps:** Some location-days return HTTP 404 (no data uploaded by the sensor). These are counted as `missing` and skipped cleanly.

---

## Team

| Name | Student ID |
|---|---|
| Burak Emre Polat | 150230328 |
| Hasan Yalçın Arıkanoğlu | 150220341 |
| Metin Furkan Amarat | 150230301 |

---

*YZV322E — Applied Data Engineering · Istanbul Technical University · Spring 2026*
