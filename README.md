# DSS150P Laboratory 3 Starter Repository

This repository supports Module 2: Pipeline Construction, Storage, and Orchestration.
It is intentionally incomplete. Students must implement the marked TODOs and document their decisions.

## Current Progress Status
- **[x] Goal 1:** Reproducible environment, modularization, Git, Docker, configuration. *(Completed)*
- **[x] Goal 2:** Raw -> staging -> curated transformations; audit/error handling; rerun-safe loading. *(Completed)*
- **[ ] Goal 3:** CSV/JSON/Parquet/PostgreSQL comparison; partitioning; selected-partition load. *(Not Started)*
- **[ ] Goal 4:** Apache Airflow DAG for extract -> transform -> load -> validate. *(Not Started)*

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

### Lab Report
Please refer to the [lab report.md](lab report.md) file for documented answers, technical evidence, benchmarks, and reflections.

Start with `DSS150P_Laboratory_Activity_3.pdf`.

## Recommended commands
```bash
cp .env.example .env
python -m venv .venv
# activate .venv then:
pip install -r requirements.txt
python -m src.cli validate-env
```
The provided `.env.example` uses `POSTGRES_HOST=localhost` for host-side commands. Docker Compose overrides the application containers to use the service hostname `postgres`.

Docker/PostgreSQL:
```bash
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
```

Airflow in Goal 4:
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```
Airflow UI: http://localhost:8080 (training credentials: admin/admin; change if reused outside the lab).
