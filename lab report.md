# DSS150P - Laboratory Activity 3 Report

## Goal 1: Reproducible Data Engineering Environments

### Task A - Build and verify a virtual environment

**1. Record the Python version and selected package versions:**
*   Python version: Python 3.11+ (Local environment)
*   `pandas==2.2.3`
*   `pyarrow==17.0.0`
*   `psycopg[binary]==3.2.3`
*   `python-dotenv==1.0.1`
*   `PyYAML==6.0.2`

**2. Explain why the virtual environment should not be committed to Git:**
*   **Platform Dependency:** Virtual environments contain binaries, compiled libraries, and executable scripts (like the Python interpreter itself) that are tied to the specific operating system and architecture they were created on (e.g., Windows x64 vs. macOS ARM). A `.venv` created on Windows will not work on Linux or macOS.
*   **Absolute Paths:** Virtual environments frequently hard-code absolute file paths to the local machine, which break as soon as the project is cloned into a different directory on another machine.
*   **Repository Bloat:** The `.venv` directory contains thousands of files and takes up significant disk space. Version control is meant for source code, not large binary dependencies. 
*   **Reproducibility:** The industry standard for reproducibility is to commit a dependency manifest (like `requirements.txt`) so that any user can reliably build a fresh, matching environment on their own system.

### Task B - Evaluate and modularize the pipeline structure
- We inspected the `src/` directory and successfully wired up `src/cli.py`. The CLI now correctly routes commands (`extract`, `transform`, `load`, `validate`, `benchmark`, `load-partition`, `run-all`) to their respective modules without duplicating any business logic, ensuring a thin orchestration layer.

### Task C - Externalize configuration and secrets
- We successfully isolated configuration and secrets by creating `.env.example` and a local `.env` file. We verified that `.env` is ignored by version control (via `.gitignore`), ensuring that sensitive credentials (like `POSTGRES_PASSWORD`) are never committed to the repository.

### Task D - Dockerfile and Docker Compose
- We built the `pipeline` Docker image and brought up the `postgres` database container. 
- The containerized `validate-env` command was successfully executed using `docker compose run --rm pipeline python -m src.cli validate-env`, which bypassed local network issues and confirmed that the environment is fully reproducible via Docker.

### Task E - Git workflow checkpoint
- A Git branch `goal1-reproducible-environment` was created.
- All configuration files and modularization changes were added and committed with the message: `feat: add reproducible pipeline environment`.

## Goal 2: ETL/ELT and Transformation Pipeline Development

### Task A - Raw extraction
- Implemented `extract_sources` in `src/extract/files.py`. It copies `customers.csv`, `products.json`, and `orders.csv` into a run-specific directory (`data/raw/run_id=<run_id>/`). This ensures raw snapshots are immutable and run-specific.

### Task B - Staging transformations
- Implemented `build_staging` in `src/transform/staging.py`.
- **Deduplication:** Applied to customers, products, and orders using `customer_id`, `product_id`, and `order_id` respectively, keeping the row with the greatest `updated_at`.
- **Normalization:** Flattened the nested `category` object in products, title-cased `city`, and lowercased `email` in customers.
- **Quarantine:** Isolated rows with missing emails, invalid prices (<= 0 or null), and invalid order quantities or statuses. Quarantined records were written to `data/quarantine/` with clear `quarantine_reason` values.

### Task C - Curated transformation
- Implemented `build_curated` in `src/transform/curated.py`.
- Joined the cleaned staging datasets and quarantined any orphan order records (missing a matching `customer_id` or `product_id` in staging).
- Computed `gross_amount`, `discount_amount`, and `net_amount`.
- Added audit columns (`pipeline_run_id`, `processed_at_utc`) and computed a `record_hash` based on business-relevant columns to track changes.

### Task D - Error handling
- Updated `src/cli.py` to wrap each major pipeline stage in `try/except` blocks.
- Exceptions now identify the specific stage (e.g., `[EXTRACT] Stage failed: ...`) and raise properly to abort the pipeline without swallowing errors, clearly separating system failures from data-quality quarantines.

### Task E - Rerun-safe PostgreSQL loading
- Implemented `upsert_curated` in `src/load/postgres.py`.
- Used `INSERT ... ON CONFLICT (order_id) DO UPDATE` to ensure safe reruns.
- Optimized the UPSERT by adding `WHERE curated.sales_order_lines.record_hash IS DISTINCT FROM EXCLUDED.record_hash` to skip updating rows whose business data hasn't changed.
- Verified idempotency: running `run-all` and then `load` multiple times resulted in exactly 49,834 total rows and 49,834 distinct `order_id`s.
