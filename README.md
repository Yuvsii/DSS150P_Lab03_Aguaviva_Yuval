# DSS150P Laboratory 3 Starter Repository

This repository supports Module 2: Pipeline Construction, Storage, and Orchestration.
It is intentionally incomplete. Students must implement the marked TODOs and document their decisions.

## Main progression
- Goal 1: reproducible environment, modularization, Git, Docker, configuration
- Goal 2: raw -> staging -> curated transformations; audit/error handling; rerun-safe loading
- Goal 3: CSV/JSON/Parquet/PostgreSQL comparison; partitioning; selected-partition load
- Goal 4: Apache Airflow DAG for extract -> transform -> load -> validate

## Architecture & Implementation Notes

### Goal 1: Reproducible Environment
- **Modular CLI:** Orchestration logic is decoupled into `src/cli.py` connecting the Extract, Transform, Load, and Validate modules.
- **Dockerization:** We successfully implemented a `pipeline` Docker container ensuring consistent execution across any OS.
- **Secrets Management:** Environment variables are safely externalized via `.env`.

### Goal 2: ETL Pipeline
- **Extract:** Raw source snapshots are copied unchanged to `data/raw/run_id=...`.
- **Transform (Staging & Curated):** 
  - Data is deduplicated, cleaned (nested JSON flattened, text normalized), and validated.
  - Bad records (missing emails, negative prices, out-of-range quantities) and orphaned references are successfully routed to `data/quarantine/`.
  - Final processed data (`gross_amount`, `net_amount`, `discount_amount`) is joined and saved to `data/curated/sales_order_lines.parquet`.
- **Load (Idempotent UPSERT):** Rerun-safe PostgreSQL loading is implemented using `INSERT ... ON CONFLICT (order_id) DO UPDATE`. A `record_hash` ensures we only update rows if the business content has actually changed.
- **Validation:** Automated quality checks verify total completeness and integrity of the final curated data.

### Goal 3: Storage Systems & Benchmarking
- **Materialization**: Wrote the curated dataset to CSV, JSON Lines, and Snappy-compressed Parquet.
- **Benchmarking**: Implemented a median-of-5 timing strategy for full and filtered reads across all formats and PostgreSQL.
- **Partitioning**: Organized the curated data into Hive-style Parquet partitions (`order_year` and `order_month`).
- **Selective Loading**: Implemented partition-specific upserts and an `audit.partition_loads` tracking table.

### Lab Report
Please refer to the [lab report.md](./lab%20report.md) file for documented answers, technical evidence, benchmarks, and reflections.

Start with `DSS150P_Laboratory_Activity_3.pdf`.

## Recommended commands
```bash
cp .env.example .env
python -m venv .venv
# activate .venv then:
pip install -r requirements.txt
```
The provided `.env.example` uses `POSTGRES_HOST=localhost` for host-side commands. Docker Compose overrides the application containers to use the service hostname `postgres`.

### Goal 1: Reproducible Environment
```bash
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
```

### Goal 2: ETL Pipeline
```bash
docker compose run --rm pipeline python -m src.cli run-all
docker compose run --rm pipeline python -m src.cli load
docker compose run --rm pipeline python -m src.cli validate
```

### Goal 3: Benchmark and Partition
```bash
docker compose run --rm pipeline python -m src.cli benchmark --repeats 5
docker compose run --rm pipeline python -m src.cli load-partition --year 2026 --month 1
```

### Goal 4: Airflow
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```
Airflow UI: http://localhost:8081 (training credentials: admin/admin; change if reused outside the lab).
