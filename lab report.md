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

### Task F - Validation
- Implemented `validate_curated` in `src/validate/quality.py` to run programmatic data quality checks.
- Validated that `order_id` values are unique and non-null, `quantity` is within the valid range (1-20), amounts are non-negative, and `status` is one of the allowed categorical values.
- Checked that all audit tracking columns (`pipeline_run_id`, `processed_at_utc`, `record_hash`) are fully populated. 
- Integrated this step into the `run-all` command so the pipeline automatically verifies data integrity at the end of the run.

### Goal 2 Acceptance Tests
- [x] Raw snapshots are run-specific and source files remain unchanged.
- [x] Duplicate business keys are resolved deterministically using latest `updated_at`.
- [x] Invalid technical records and orphan references are quarantined with reasons.
- [x] Curated amounts are calculated and audit columns are populated.
- [x] Validation detects duplicate/null business keys and invalid amounts/statuses.
- [x] Repeated load does not create duplicate `order_id` values.

### Partitioned Parquet Explanation
**How partitioning reduces unnecessary I/O:**
When querying a partitioned Parquet dataset with a filter on a partition key (e.g., `order_year=2026`), the query engine utilizes "partition pruning" (or "filter pushdown"). Because the directory structure itself encodes the partition values, the engine completely ignores the directories/files that do not match the filter. This drastically reduces the amount of Disk I/O, as the engine doesn't even have to open or scan the irrelevant files, leading to significantly faster queries.

### Selective Partition Load Deduplication
When the partition load is rerun for `2026-09`, the `audit.partition_loads` table updates its `loaded_at_utc` timestamp without creating duplicate entries (due to the `ON CONFLICT (partition_key) DO UPDATE` logic). Likewise, the business rows in `curated.sales_order_lines` remain completely deduplicated because `upsert_curated` uses `ON CONFLICT (order_id) DO UPDATE` combined with the `record_hash` check, preventing identical rows from being needlessly updated or duplicated.

### Goal 3 Analysis Questions

*(Context: Benchmarks were run on Linux 6.18.33.2-microsoft-standard-WSL2 (x86_64) using Python 3.11.16 via Docker, reporting medians across 5 iterations).*

**1. Which file format was smallest on your machine, and what encoding/compression characteristics help explain the result?**
Parquet was the smallest (5.4 MB), compared to CSV (15.2 MB) and JSON Lines (29.9 MB). This is because Parquet is a columnar format; storing values of the same type together allows for highly efficient dictionary encoding and run-length encoding. Additionally, it applies built-in block compression (like `snappy`) which significantly shrinks the footprint compared to text-based formats.

**2. Which representation was fastest for a full dataset read? Does that imply it is best for every workload?**
Parquet was the fastest for a full dataset read (median 0.09s), easily beating CSV (0.57s), JSONL (1.30s), and PostgreSQL (0.75s). However, this does not mean it is best for every workload. Parquet is immutable and designed for analytical workloads (OLAP) requiring bulk reads and column aggregations. For transactional workloads (OLTP) requiring rapid single-row inserts, updates, or point lookups, PostgreSQL is vastly superior due to its row-oriented engine and ACID guarantees.

**3. How did filtered retrieval differ between Parquet and PostgreSQL? What additional PostgreSQL design (such as an index) could change the result?**
For filtered retrieval (`status='DELIVERED'`), Parquet took 0.08s while PostgreSQL took 0.13s. Parquet utilizes predicate pushdown and column statistics to completely skip over blocks of data that don't match the filter. PostgreSQL, lacking an index on `status`, had to perform a full sequential table scan. If we added a B-Tree index on the `status` column in PostgreSQL, the query planner could instantly locate the matching rows via the index, potentially making the PostgreSQL query faster than Parquet for highly selective queries.

**4. Why is JSON Lines generally more pipeline-friendly than one giant JSON array for append/stream-oriented processing?**
JSON Lines (JSONL) stores each record as a completely valid JSON object on a new line (`\n`). This makes it extremely streamable—you can read, process, or append data line-by-line without loading the entire file into memory. A giant JSON array requires opening `[` and closing `]` brackets, meaning appending a new record requires modifying the end of the file, and parsing it often requires loading the entire array structure into memory.

**5. What happens if a partition key has extremely high cardinality or poor query locality?**
If a partition key has extremely high cardinality (e.g., partitioning by `order_id` or a precise `timestamp`), the system will generate thousands or millions of tiny partition folders and files. This is known as the "small file problem" and creates massive metadata overhead for the file system and query engines, destroying performance. Partition keys should group data into reasonably large, uniform chunks (like `year` and `month`) that match common query patterns.

### Goal 3 Acceptance Tests
- [x] CSV, JSONL, and Parquet represent the same logical row set. *(Verified: All formats return exactly 8,355 rows for the filtered test).*
- [x] Benchmark uses repeated measurements and reports medians.
- [x] Results include file size where meaningful and do not equate size alone with quality.
- [x] Partitioned Parquet is organized by year/month. *(Verified: `data/partitioned/order_year=2026/order_month=9/` exists).*
- [x] A selected partition can be loaded and audited in PostgreSQL. *(Verified: Loaded 951 rows for partition 2026-09 and successfully updated `audit.partition_loads`).*

## Goal 4: Workflow Orchestration and Scheduling with Apache Airflow

### Task B - Complete DAG operational configuration
*   **Schedule (`0 2 * * *`):** Running the pipeline daily at 2:00 AM UTC is appropriate because it ensures the previous day's e-commerce transactions are fully settled and available. It also executes during low-traffic off-hours, minimizing database contention.
*   **Catch-up Behavior (`catchup=False`):** Catchup is disabled to prevent Airflow from automatically triggering hundreds of historical DAG runs when the DAG is first turned on (since `start_date` is in the past). We want to control historical backfills manually.

### Task E - Deliberate failure and recovery
To test recovery, we temporarily renamed `orders.csv` to `orders_hidden.csv`. 
*   **Failure:** The DAG failed at the `extract` task. The failure callback printed the exact exception (`FileNotFoundError`). Airflow automatically retried twice with a 1-minute delay, as configured.
*   **Recovery:** We restored the file name and cleared the failed `extract` task in the Airflow UI. Airflow successfully resumed execution from the `extract` task.
*   **Safety:** It is completely safe to rerun because our `load` task uses idempotent PostgreSQL UPSERTs (with `ON CONFLICT (order_id)` and `record_hash` checking), guaranteeing that no duplicate business rows are created during retries.

### Optional challenge - backfill reasoning
To backfill a historical month (e.g., January 2026), we would trigger a manual parameterized run in the Airflow UI with `{"run_mode": "partition", "year": 2026, "month": 1}`. 
Because our `load-partition` pipeline is idempotent, we can safely overwrite historical months without risk of double-loading. We avoid data duplication by relying on our `record_hash` UPSERT strategy in the data warehouse, meaning we do not need to manually delete the old partition before rerunning.

### Goal 4 Acceptance Tests
- [x] Airflow imports the DAG without parse errors.
- [x] DAG has explicit schedule, parameters, dependencies, retries, timeout, and catchup behavior.
- [x] Full and partition-mode runs can be observed in Airflow.
- [x] Controlled failure produces visible retries/failure handling.
- [x] Recovery succeeds without manual database cleanup or duplicate business rows.
- [x] DAG code delegates actual pipeline logic to reusable modules/CLI.
- [x] One `pipeline_run_id` is propagated consistently across tasks in the same DAG run.

## Technical Questions

**1. Why is `record_hash` useful for rerun-safe loading, and which columns should not be included in it?**
A `record_hash` allows the database to instantly verify if an incoming row actually contains modified business data compared to the existing row. If the hash matches, the UPSERT can be skipped, saving massive amounts of I/O and transaction log bloat. You should **never** include audit columns (like `processed_at_utc` or `pipeline_run_id`) in the hash, because these change on every single run; including them would trick the database into thinking the business data changed, causing useless updates on every rerun.

**2. Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?**
Raw data serves as the immutable source of truth. If a bug in the transformation logic is discovered months later, or if a new business requirement requires parsing a previously ignored column, preserving the raw data allows us to completely recalculate and backfill the staging/curated layers from scratch.

**3. What is the difference between a data-quality rejection and a system exception?**
A data-quality rejection occurs when the system is working perfectly, but the *data* violates a business rule (e.g., negative price). The row is safely routed to a quarantine folder, and the pipeline continues. A system exception occurs when the code or infrastructure fails (e.g., missing file, database offline, memory error). The pipeline must immediately abort to prevent corruption.

**4. Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?**
Parquet uses a columnar layout, meaning a query that only selects 2 columns (out of 50) only reads those 2 columns from disk, whereas CSV must read the entire file line-by-line. Parquet also uses strong data types (eliminating expensive string parsing) and applies aggressive dictionary encoding and block compression.

**5. Why is a DAG that contains all transformation logic directly considered harder to maintain?**
If a DAG contains business logic (e.g., Pandas transformations directly inside a `PythonOperator`), the code becomes tightly coupled to Airflow. It cannot be run locally, it cannot be unit tested without a full Airflow environment, and it cannot be triggered by other schedulers. By keeping the DAG as a "thin orchestrator" that simply calls `src.cli`, the pipeline remains portable.

**6. How do retries interact with idempotency? Give an example where retries without idempotency cause damage.**
Retries are only safe if the task is idempotent (meaning running it 1 time or 100 times produces the exact same final state). If a `load` task uses standard `INSERT` statements and crashes halfway through, Airflow will retry the task. Without idempotency, the retry will `INSERT` the first half of the rows *again*, causing massive data duplication.

**7. What trade-off is introduced by partitioning too aggressively?**
Partitioning too aggressively (e.g., partitioning by `hour` or `order_id` on a small dataset) creates the "small file problem." The file system will generate thousands of tiny partition folders and files. The query engine will then spend significantly more time opening files and reading metadata than actually scanning data, destroying read performance.

**8. How would you adapt the pipeline if the source became an API or database instead of local files?**
Thanks to our modular architecture, we would only need to rewrite the `src/extract/files.py` module to fetch data from the API/Database and save the raw response to the `data/raw/run_id=...` folder as a CSV/JSON file. The `staging`, `curated`, and `load` modules would not need to change at all, as they are decoupled from extraction.

## AI Tool Use and Academic Integrity
In accordance with the course policy, Generative AI (Gemini/Claude) was utilized during this laboratory activity strictly as an aid for debugging complex errors (such as Docker port conflicts and Python dependency issues), generating boilerplate pipeline structure, and refining explanations of Data Engineering concepts (e.g., UPSERT idempotency and Parquet partitioning). 
All final architectural decisions, pipeline workflows, and code logic were reviewed, understood, and successfully executed by the student on their local machine to ensure complete comprehension.

## Final Submission Checklist

### Checklist A
- [x] No .env/secrets committed.
- [x] Source files unchanged.
- [x] Rerun-safe PostgreSQL load verified.
- [x] Partitioned Parquet and selected-partition load verified.
- [x] Git history includes Goal 1-4 checkpoints.

### Checklist B
- [x] All required commands documented in README.
- [x] Staging/curated/quarantine outputs reproducible.
- [x] Benchmark results and interpretation included.
- [x] Airflow full/partition/failure/recovery evidence included.
- [x] Repository runs without relying on undocumented manual edits
