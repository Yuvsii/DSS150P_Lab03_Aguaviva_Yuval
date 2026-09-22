import logging
import psycopg  # pyrefly: ignore [missing-import]
from src.config import DB

logger = logging.getLogger(__name__)


def _get_connection():
    """Create a psycopg connection from config."""
    return psycopg.connect(
        host=DB['host'],
        port=DB['port'],
        dbname=DB['dbname'],
        user=DB['user'],
        password=DB['password'],
    )


def upsert_curated(df, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics.

    Requirement: order_id is the conflict key. A rerun with unchanged records
    must not create duplicate business keys. Uses record_hash to skip
    unnecessary updates when business content is unchanged.
    """
    if df is None or df.empty:
        logger.warning('[LOAD] No data to load')
        return 0

    # Columns matching the DB schema (exclude order_year, order_month which are not in the table)
    db_cols = [
        'order_id', 'customer_id', 'product_id', 'order_timestamp',
        'customer_city', 'customer_tier', 'product_name', 'category', 'brand',
        'quantity', 'unit_price', 'discount_pct',
        'gross_amount', 'discount_amount', 'net_amount',
        'status', 'source_updated_at',
        'pipeline_run_id', 'processed_at_utc', 'record_hash'
    ]

    upsert_sql = f"""
    INSERT INTO curated.sales_order_lines ({', '.join(db_cols)})
    VALUES ({', '.join(['%s'] * len(db_cols))})
    ON CONFLICT (order_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        product_id = EXCLUDED.product_id,
        order_timestamp = EXCLUDED.order_timestamp,
        customer_city = EXCLUDED.customer_city,
        customer_tier = EXCLUDED.customer_tier,
        product_name = EXCLUDED.product_name,
        category = EXCLUDED.category,
        brand = EXCLUDED.brand,
        quantity = EXCLUDED.quantity,
        unit_price = EXCLUDED.unit_price,
        discount_pct = EXCLUDED.discount_pct,
        gross_amount = EXCLUDED.gross_amount,
        discount_amount = EXCLUDED.discount_amount,
        net_amount = EXCLUDED.net_amount,
        status = EXCLUDED.status,
        source_updated_at = EXCLUDED.source_updated_at,
        pipeline_run_id = EXCLUDED.pipeline_run_id,
        processed_at_utc = EXCLUDED.processed_at_utc,
        record_hash = EXCLUDED.record_hash
    WHERE curated.sales_order_lines.record_hash IS DISTINCT FROM EXCLUDED.record_hash
    """

    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            rows = df[db_cols].values.tolist()

            # Convert pandas types to Python native types for psycopg
            clean_rows = []
            for row in rows:
                clean_row = []
                for val in row:
                    if hasattr(val, 'isoformat'):
                        clean_row.append(val.isoformat() if not hasattr(val, 'to_pydatetime') else val.to_pydatetime())
                    elif hasattr(val, 'item'):
                        clean_row.append(val.item())
                    else:
                        clean_row.append(val)
                clean_rows.append(tuple(clean_row))

            cur.executemany(upsert_sql, clean_rows)
            row_count = cur.rowcount if cur.rowcount >= 0 else len(clean_rows)

        conn.commit()
        logger.info('[LOAD] Upserted %d rows into curated.sales_order_lines', len(clean_rows))
        return len(clean_rows)
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f'[LOAD] Failed to upsert curated data: {e}') from e
    finally:
        conn.close()


def load_partition(df, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    if df is None or df.empty:
        logger.warning('[LOAD-PARTITION] No data to load')
        return 0

    # Filter to partition
    partition = df[(df['order_year'] == year) & (df['order_month'] == month)]
    if partition.empty:
        logger.warning('[LOAD-PARTITION] No data for partition %d-%02d', year, month)
        return 0

    logger.info('[LOAD-PARTITION] Loading %d rows for partition %d-%02d', len(partition), year, month)

    # Upsert the partition data
    count = upsert_curated(partition, run_id)

    # Record partition load in audit table
    partition_key = f'{year}-{month:02d}'
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id)
                VALUES (%s, NOW() AT TIME ZONE 'UTC', %s, %s)
                ON CONFLICT (partition_key) DO UPDATE SET
                    loaded_at_utc = EXCLUDED.loaded_at_utc,
                    row_count = EXCLUDED.row_count,
                    pipeline_run_id = EXCLUDED.pipeline_run_id
            """, (partition_key, len(partition), run_id))
        conn.commit()
        logger.info('[LOAD-PARTITION] Recorded audit for partition %s', partition_key)
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f'[LOAD-PARTITION] Failed to record partition audit: {e}') from e
    finally:
        conn.close()

    return count
