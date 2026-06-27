# E-commerce Medallion Architecture (Olist)

A local, containerized **data engineering pipeline** that ingests the Brazilian
Olist e-commerce dataset and processes it through a **medallion architecture**
(landing → bronze → silver → gold) using object storage, Spark, and Delta Lake,
orchestrated with Airflow.

> Portfolio project — runs entirely on Docker, no cloud account required.

---

## Tech stack

| Layer | Tool |
|---|---|
| Object storage (data lake) | **MinIO** (S3-compatible) |
| Processing engine | **Apache Spark 3.5** (PySpark) |
| Table format | **Delta Lake 3.0** |
| Orchestration | **Apache Airflow 3.0** |
| Metadata DB (Airflow) | **PostgreSQL 15** |
| Dev / job runtime | **Jupyter (pyspark-notebook)** |
| Data quality | **Great Expectations** *(partial)* |

---

## Architecture flow

```mermaid
flowchart LR
    subgraph SRC["Source"]
        CSV["data/*.csv<br/>(9 Olist datasets)"]
    end

    subgraph MINIO["MinIO (S3 data lake)"]
        LAND["landingzone/<br/>dataset/yyyy/mm/dd/*.csv"]
        BRONZE["bronze/<br/>Delta tables"]
        SILVER["silver/<br/>(planned)"]
        GOLD["gold/<br/>(planned)"]
    end

    CSV -->|"upload_to_landing.py<br/>(boto3)"| LAND
    LAND -->|"landing_to_bronze.py<br/>(Spark Structured Streaming)"| BRONZE
    BRONZE -.->|"planned: cast, dedup, clean"| SILVER
    SILVER -.->|"planned: joins, aggregates"| GOLD

    AIRFLOW["Airflow DAG<br/>ecommerce_medallion_pipeline"] -.orchestrates.-> CSV
    JUP["Jupyter<br/>(runs Spark jobs)"] -.executes.-> BRONZE
```

### Medallion layers

| Layer | What it holds | Status |
|---|---|---|
| **Landing** | Raw CSVs, partitioned `dataset/yyyy/mm/dd/` | ✅ |
| **Bronze** | Raw-as-Delta; all columns string + `ingestion_date` + `source_file_name` lineage | ✅ |
| **Silver** | Cleaned/conformed per table: typed, deduplicated, trimmed, null-handled | 🚧 planned |
| **Gold** | Business-level joins & aggregates | 🚧 planned |

---

## Data flow (step by step)

1. **Ingestion** — [`ingestion/upload_to_landing.py`](ingestion/upload_to_landing.py)
   uses `boto3` to upload every CSV in `data/` to the MinIO `landingzone` bucket,
   keyed as `dataset_name/year/month/day/file.csv`.

2. **Landing → Bronze** — [`spark/jobs/landing_to_bronze.py`](spark/jobs/landing_to_bronze.py)
   reads a dataset from landing with **Spark Structured Streaming**
   (`trigger(availableNow=True)` + checkpoints for incremental loads), adds
   `ingestion_date` and `source_file_name`, and writes a **Delta** table to
   `s3a://bronze/<dataset>`.

3. **Orchestration** — [`airflow/dags/ecommerce_medallion_dag.py`](airflow/dags/ecommerce_medallion_dag.py)
   runs `upload_to_landing`, then **fans out** one bronze task per dataset in
   parallel (bounded by `max_active_tasks=2`). Tasks shell into the jupyter
   container via `docker exec` to run the Spark jobs.

```
upload_to_landing ─┬─> landing_to_bronze_olist_customers_dataset
                   ├─> landing_to_bronze_olist_orders_dataset
                   ├─> landing_to_bronze_olist_order_items_dataset
                   └─> ... (one per dataset, max 2 at a time)
```

---

## Project structure

```
ecommerce-medallion-setup/
├── docker-compose.yml          # all services
├── .env / .env.example         # credentials & config
├── data/                       # source CSVs (gitignored)
├── ingestion/
│   └── upload_to_landing.py    # CSV -> MinIO landing
├── spark/
│   ├── jobs/
│   │   ├── landing_to_bronze.py
│   │   ├── bronze_quality_checks.ipynb
│   │   └── gx/                 # Great Expectations project
│   └── delta/                  # local Delta output / checkpoints (gitignored)
├── airflow/
│   ├── dags/ecommerce_medallion_dag.py
│   └── logs/                   # (gitignored)
├── docker/jupyter/             # custom Jupyter image (Dockerfile + requirements)
├── README.md
└── KNOWN_ISSUES.md             # Windows/Docker Desktop gotchas + fixes
```

---

## Prerequisites

- Docker Desktop (with Compose v2)
- The 9 Olist CSVs in `data/` — download the Brazilian E-Commerce dataset from
  [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place
  all CSV files in the `data/` folder before running the pipeline.

---

## Setup & run

1. **Configure environment**
   ```bash
   cp .env.example .env
   ```
   Fill in values (defaults below work for local dev):
   ```
   MINIO_ACCESS_KEY=minioadmin
   MINIO_SECRET_KEY=minioadmin
   BUCKET_NAME=landingzone
   POSTGRES_USER=airflow
   POSTGRES_PASSWORD=airflow
   POSTGRES_DB=airflow
   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
   AIRFLOW__CORE__EXECUTOR=LocalExecutor
   AIRFLOW__CORE__LOAD_EXAMPLES=False
   ```

2. **Start the stack**
   ```bash
   docker compose up -d
   docker compose ps        # wait until services are healthy
   ```

3. **Run the pipeline** — open the Airflow UI (see below), unpause
   `ecommerce_medallion_pipeline`, and trigger it. Or from the CLI:
   ```bash
   docker exec airflow airflow dags trigger ecommerce_medallion_pipeline
   ```

4. **Inspect results** — open the MinIO console and browse the `bronze` bucket.

---

## Services & ports

| Service | URL | Notes |
|---|---|---|
| Airflow UI | http://localhost:8082 | user `admin`; password regenerated each restart¹ |
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| MinIO S3 API | http://localhost:9000 | |
| Jupyter Lab | http://localhost:8888 | |
| Spark master UI | http://localhost:8080 | |

¹ Get the Airflow password:
```bash
docker exec airflow cat $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated
```

---

## Resource tuning (parallelism)

Bronze tasks run independent Spark jobs inside the jupyter container, so
concurrency is capped to avoid memory blow-ups:

- `max_active_tasks=2` in the DAG — at most 2 bronze tasks at once.
- `local[2]`, `spark.driver.memory=1g`, `spark.sql.shuffle.partitions=8` per job.

Raise `max_active_tasks` gradually while watching `docker stats`.

---

## Roadmap

- [ ] **Silver layer** — batch read bronze Delta → cast types, deduplicate, clean
      strings, handle nulls; write `s3a://silver/<dataset>` (overwrite or MERGE).
- [ ] **Gold layer** — business marts (joins + aggregates).
- [ ] Wire **Great Expectations** validations into the Airflow DAG.
- [ ] Expand bronze quality checks beyond the customers suite.

---

## Troubleshooting

See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) — covers the Airflow 3.0 "DAG disappears
from the UI" problem (Windows/Docker Desktop sleep), paused-at-creation behavior,
and parallel-task memory limits.
