# GitPulse 

GitPulse is a real-time and batch data platform that ingests GitHub's public event stream (GH Archive hourly dumps), lands it in an S3-compatible Parquet lakehouse, and builds a Type-2 slowly changing dimensional (SCD) warehouse. The pipeline is orchestrated via Apache Airflow, validated with 40+ automated data quality tests, monitored on a Grafana dashboard, and analyzed via a Prophet time-series anomaly detection layer.

This repository is built with production-grade engineering practices, designed as a portfolio showcase to demonstrate modern data lakehouse, analytics engineering, and ML-ops patterns.

---

## System Architecture

```mermaid
graph TD
    %% Ingestion
    subgraph Ingestion Layer [1. Ingestion Layer]
        GHA[GH Archive HTTP] -->|Batch: Spark| S3[(MinIO / S3 Raw Zone)]
        GHA_Live[GH Archive Live] -->|Stream: Python Producer| RP[Redpanda / Kafka]
        RP -->|Python Consumer| S3
    end

    %% Storage & Warehouse
    subgraph Lakehouse & Warehouse [2. Storage & Warehouse]
        S3 -->|Airflow Loader| PG_Stg[(Postgres Raw/Staging)]
        PG_Stg -->|dbt Staging| dbt_stg[dbt Staging: views]
        dbt_stg -->|dbt Intermediate| dbt_int[dbt Intermediate: views]
        dbt_int -->|dbt Marts| dbt_marts[dbt Marts: tables]
    end

    %% Downstream & BI
    subgraph Downstream Layer [3. Analytics & Observability]
        dbt_marts -->|Prophet Anomaly Detector| PG_Anom[(Anomaly Table)]
        dbt_marts -->|Grafana Dashboard| Grafana[Grafana Visualization]
        PG_Anom -->|Grafana Dashboard| Grafana
    end

    %% Orchestration
    subgraph Orchestration [Orchestration]
        Airflow[Airflow Orchestrator] -->|Triggers & Controls| Ingestion
        Airflow -->|Runs Transformations| dbt_marts
        Airflow -->|Logs Stats| Grafana
    end
```

---

## Database ERD (Dimensional Model)

The warehouse features a Type-2 Slowly Changing Dimension (SCD) for repositories (`dim_repo`) and a star-schema marts layer optimized for downstream analytics.

```mermaid
erDiagram
    RAW_EVENTS {
        bigint id PK
        varchar type
        text actor
        text repo
        text org
        text payload
        boolean public
        varchar created_at
        timestamp loaded_at
    }

    DIM_ACTOR {
        bigint actor_id PK
        varchar actor_login
        varchar actor_avatar_url
        bigint total_events_created
        bigint push_events_created
        bigint watch_events_created
        bigint pr_events_created
        bigint issues_events_created
        timestamp last_event_at
    }

    DIM_REPO {
        bigint repo_id PK
        varchar repo_name
        varchar primary_language
        text description
        varchar star_count_bucket
        timestamp valid_from
        timestamp valid_to
        boolean is_current
    }

    DIM_DATE {
        date date_id PK
        date date_day
        integer year
        integer month
        integer day
        integer day_of_week
        varchar day_name
        integer quarter
        boolean is_weekend
    }

    FACT_EVENTS {
        bigint event_id PK
        varchar event_type
        bigint actor_id FK
        bigint repo_id FK
        date date_id FK
        timestamp event_at
        boolean is_public
        timestamp loaded_at
    }

    FCT_DAILY_REPO_METRICS {
        bigint repo_id FK
        varchar repo_name
        date event_date
        bigint total_events
        bigint star_count
        bigint push_count
        bigint pr_count
        bigint issue_count
    }

    DETECTED_ANOMALIES {
        integer id PK
        bigint repo_id FK
        varchar repo_name
        date ds
        double y
        double yhat_lower
        double yhat_upper
        double anomaly_score
        timestamp run_timestamp
    }

    PIPELINE_METRICS {
        integer id PK
        varchar run_id
        timestamp dag_run_timestamp
        bigint rows_ingested
        integer freshness_lag_seconds
        integer dbt_tests_passed
        integer dbt_tests_failed
        integer duration_seconds
        varchar status
        timestamp run_timestamp
    }

    DIM_ACTOR ||--o{ FACT_EVENTS : "creates"
    DIM_REPO ||--o{ FACT_EVENTS : "associates"
    DIM_DATE ||--o{ FACT_EVENTS : "occurs_on"
    DIM_REPO ||--o{ FCT_DAILY_REPO_METRICS : "aggregates"
    FCT_DAILY_REPO_METRICS ||--o{ DETECTED_ANOMALIES : "forecasts"
```

---

## Tech Stack & Setup Instructions

Every component is containerized and launches using a single Docker Compose network.

### Prerequisites
- Docker & Docker Compose (allocated $\ge$ 8GB RAM for spark and prophet run)
- Python 3.10+ (for optional local run)

### Spin up the Platform
```bash
docker compose up --build -d
```
This launches:
1. **PostgreSQL** (`localhost:5432`): Hosts the data warehouse (`gitpulse` DB) and Airflow metadata.
2. **MinIO** (`localhost:9000` / `9001`): Local S3-compatible storage. Initializes `gitpulse-lake` bucket automatically.
3. **Redpanda** (`localhost:9092`): High-performance streaming broker.
4. **Redpanda Console** (`localhost:8081`): Web UI to inspect topics and offsets.
5. **Apache Airflow** (`localhost:8080`): DAG Scheduler and Web UI (Admin user: `admin` / password: `admin`).
6. **Grafana** (`localhost:3000`): Observability Dashboard (Pre-loaded with PostgreSQL datasource and dashboards).

### Triggering Ingestion & Transformations
Open the Airflow Web UI at [http://localhost:8080](http://localhost:8080), unpause the `gitpulse_orchestration` DAG, and trigger a run.
To run a manual PySpark backfill for a date range:
```bash
docker exec -it gitpulse-airflow-scheduler \
  python3 /opt/airflow/src/batch/backfill.py --start-date 2026-08-11 --end-date 2026-08-11 --hour 14
```

---

## Data Model & SCD Type-2 Logic
- **Exactly-Once Semantics**: To support idempotent replays, raw ingestion records carry the GitHub Archive `id` as primary key. The loader executes:
  ```sql
  INSERT INTO raw.github_events (...)
  SELECT ... FROM temp_staging_events
  ON CONFLICT (id) DO NOTHING;
  ```
- **Type-2 SCD**: Handled via dbt snapshots (`snapshots/dim_repo_snapshot.sql`). Whenever a repository's `primary_language`, `description`, or `star_count_bucket` changes, dbt automatically closes out the active record by setting its `valid_to` date and inserts a new row with `is_current = True`.

---

## Infrastructure as Code (IaC)

A complete production module is structured inside `/terraform` targeting AWS. It provisions:
- Encrypted and versioned S3 bucket for raw/processed zones.
- Amazon Redshift Serverless (Namespace and Workgroup).
- Dedicated IAM Roles/Policies allowing Redshift Spectrum to scan the S3 data lake via Glue Data Catalog.

To plan it (requires AWS credentials):
```bash
cd terraform
terraform init
terraform plan
```

---

## Production Scalability Considerations ("What I'd change for Prod")

In a live interview, here is how I would scale this architecture to support billions of events:

1. **Streaming Ingestion**: 
   - Replace the single Python Consumer/Producer with **Apache Flink** or **Spark Streaming** running on Amazon EMR or EKS. This enables checkpointing, distributed partitioning, and watermarking for late-arriving events.
2. **Storage Partitioning**:
   - Utilize Apache Iceberg or Delta Lake on top of AWS S3 instead of plain Parquet files. This allows ACID transactions, schema evolution, and file layout optimization (e.g., Z-ordering by `repo_id` to accelerate search queries).
3. **Data Warehouse (Redshift Optimization)**:
   - Configure **distribution keys** (`DISTKEY`) on `repo_id` or `actor_id` across `fact_events` and dimensions to avoid network shuffles during massive joins.
   - Set **sort keys** (`SORTKEY`) on `event_at` and `date_id` to speed up range scans.
4. **dbt Scaling**:
   - Convert all intermediate and marts models to **incremental materializations** rather than full table rebuilds.
   - Run dbt inside an ECS task triggered by Airflow's `EcsRunTaskOperator` instead of executing it on the Airflow scheduler container directly, separating compute from orchestration.
5. **Prophet Anomaly Detection at Scale**:
   - Running Prophet sequentially for thousands of repos inside a python container does not scale. I would write a **Spark Pandas UDF** (User Defined Function) running on a distributed EMR cluster. This enables running Prophet models for millions of repositories in parallel using group-by partition splits.
6. **Pipeline Observability**:
   - Shift from writing execution stats to a Postgres table to exporting StatsD metrics directly from Airflow and dbt into Datadog, triggering pager alerts on SLA breaches.
