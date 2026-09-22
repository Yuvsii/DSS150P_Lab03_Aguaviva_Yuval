import logging
import time
import statistics
import pandas as pd
from pathlib import Path
from src.load.postgres import _get_connection

logger = logging.getLogger(__name__)

def _measure_time(func, *args, **kwargs):
    start = time.perf_counter()
    res = func(*args, **kwargs)
    end = time.perf_counter()
    return (end - start), res

def run_benchmark(curated_dir: str | Path, benchmark_dir: str | Path, repeats: int = 5):
    """Run Goal 3 Task A & B: materialize files, run timings, and report results."""
    curated_dir = Path(curated_dir)
    benchmark_dir = Path(benchmark_dir)
    benchmark_dir.mkdir(parents=True, exist_ok=True)

    logger.info('[BENCHMARK] Starting benchmark (repeats=%d)', repeats)
    source_path = curated_dir / 'sales_order_lines.parquet'
    
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")
        
    # Read the data to write out
    df = pd.read_parquet(source_path)
    logger.info('[BENCHMARK] Loaded %d rows from source for materialization', len(df))

    results = []

    # 1. CSV
    csv_path = benchmark_dir / 'sales.csv'
    write_time, _ = _measure_time(df.to_csv, csv_path, index=False)
    file_size = csv_path.stat().st_size
    
    read_times = []
    filtered_times = []
    for _ in range(repeats):
        rt, df_read = _measure_time(pd.read_csv, csv_path)
        read_times.append(rt)
        
        def _read_and_filter():
            temp = pd.read_csv(csv_path)
            return temp[temp['status'] == 'DELIVERED']
            
        ft, df_filtered = _measure_time(_read_and_filter)
        filtered_times.append(ft)
        
    results.append({
        'Format': 'CSV',
        'Size (bytes)': file_size,
        'Write Time (s)': write_time,
        'Full Read Median (s)': statistics.median(read_times),
        'Filtered Read Median (s)': statistics.median(filtered_times),
        'Rows': len(df_filtered)
    })

    # 2. JSON Lines
    json_path = benchmark_dir / 'sales.jsonl'
    write_time, _ = _measure_time(df.to_json, json_path, orient='records', lines=True)
    file_size = json_path.stat().st_size
    
    read_times = []
    filtered_times = []
    for _ in range(repeats):
        rt, df_read = _measure_time(pd.read_json, json_path, orient='records', lines=True)
        read_times.append(rt)
        
        def _read_and_filter():
            temp = pd.read_json(json_path, orient='records', lines=True)
            return temp[temp['status'] == 'DELIVERED']
            
        ft, df_filtered = _measure_time(_read_and_filter)
        filtered_times.append(ft)

    results.append({
        'Format': 'JSONL',
        'Size (bytes)': file_size,
        'Write Time (s)': write_time,
        'Full Read Median (s)': statistics.median(read_times),
        'Filtered Read Median (s)': statistics.median(filtered_times),
        'Rows': len(df_filtered)
    })

    # 3. Parquet
    parquet_path = benchmark_dir / 'sales.parquet'
    write_time, _ = _measure_time(df.to_parquet, parquet_path, compression='snappy', index=False)
    file_size = parquet_path.stat().st_size
    
    read_times = []
    filtered_times = []
    for _ in range(repeats):
        rt, df_read = _measure_time(pd.read_parquet, parquet_path)
        read_times.append(rt)
        
        # Parquet supports pushdown filters
        ft, df_filtered = _measure_time(pd.read_parquet, parquet_path, filters=[('status', '==', 'DELIVERED')])
        filtered_times.append(ft)

    results.append({
        'Format': 'Parquet',
        'Size (bytes)': file_size,
        'Write Time (s)': write_time,
        'Full Read Median (s)': statistics.median(read_times),
        'Filtered Read Median (s)': statistics.median(filtered_times),
        'Rows': len(df_filtered)
    })

    # 4. PostgreSQL
    # It is already loaded. We just query it.
    pg_conn = _get_connection()
    try:
        # Get table size
        with pg_conn.cursor() as cur:
            cur.execute("SELECT pg_total_relation_size('curated.sales_order_lines');")
            pg_size = cur.fetchone()[0]

        def _pg_full_read():
            with pg_conn.cursor() as cur:
                cur.execute("SELECT * FROM curated.sales_order_lines;")
                return cur.fetchall()

        def _pg_filtered_read():
            with pg_conn.cursor() as cur:
                cur.execute("SELECT * FROM curated.sales_order_lines WHERE status = 'DELIVERED';")
                return cur.fetchall()

        read_times = []
        filtered_times = []
        for _ in range(repeats):
            rt, rows_all = _measure_time(_pg_full_read)
            read_times.append(rt)
            
            ft, rows_filtered = _measure_time(_pg_filtered_read)
            filtered_times.append(ft)

        results.append({
            'Format': 'PostgreSQL',
            'Size (bytes)': pg_size,
            'Write Time (s)': 0.0, # Already loaded
            'Full Read Median (s)': statistics.median(read_times),
            'Filtered Read Median (s)': statistics.median(filtered_times),
            'Rows': len(rows_filtered)
        })

    finally:
        pg_conn.close()

    import platform
    print("\n--- HARDWARE/OS CONTEXT ---")
    print(f"System: {platform.system()} {platform.release()} ({platform.machine()})")
    print(f"Python: {platform.python_version()}")

    results_df = pd.DataFrame(results)
    print("\n--- BENCHMARK RESULTS ---")
    print(results_df.to_string(index=False))
    
    out_csv = benchmark_dir / 'benchmark_results.csv'
    results_df.to_csv(out_csv, index=False)
    logger.info('[BENCHMARK] Saved results to %s', out_csv)
