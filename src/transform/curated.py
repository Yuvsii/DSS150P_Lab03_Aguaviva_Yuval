import logging
import pandas as pd
from src.config import path_for
from src.common.audit import utc_now_iso, record_hash

logger = logging.getLogger(__name__)

HASH_KEYS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp',
    'quantity', 'unit_price', 'discount_pct', 'status'
]


def build_curated(staging: dict, run_id: str):
    """Join staging orders/customers/products and create analysis-ready sales rows.

    Required columns include gross_amount, discount_amount, net_amount,
    processed_at_utc, pipeline_run_id, and record_hash.

    Orphan customer/product references must be quarantined, not silently dropped.
    """
    orders = staging['orders']
    customers = staging['customers']
    products = staging['products']

    logger.info('[CURATED] Starting curated build: %d orders, %d customers, %d products',
                len(orders), len(customers), len(products))

    # Identify orphans BEFORE joining
    orphan_customer = ~orders['customer_id'].isin(customers['customer_id'])
    orphan_product = ~orders['product_id'].isin(products['product_id'])

    quarantine_frames = []

    if orphan_customer.sum() > 0:
        q = orders[orphan_customer].copy()
        q['quarantine_reason'] = 'orphan_customer'
        q['source_entity'] = 'orders'
        quarantine_frames.append(q)
        logger.info('[CURATED] Quarantined %d orders (orphan_customer)', len(q))

    if orphan_product.sum() > 0:
        q = orders[orphan_product & ~orphan_customer].copy()
        q['quarantine_reason'] = 'orphan_product'
        q['source_entity'] = 'orders'
        quarantine_frames.append(q)
        logger.info('[CURATED] Quarantined %d orders (orphan_product)', len(q))

    # Write curated-stage quarantine
    if quarantine_frames:
        quarantine_df = pd.concat(quarantine_frames, ignore_index=True)
        quarantine_dir = path_for('quarantine_dir')
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        quarantine_path = quarantine_dir / f'quarantine_curated_{run_id}.csv'
        quarantine_df.to_csv(quarantine_path, index=False)
        logger.info('[CURATED] Wrote %d curated quarantine records to %s', len(quarantine_df), quarantine_path)

    # Keep only valid orders (no orphans)
    valid_orders = orders[~orphan_customer & ~orphan_product].copy()

    # Join with customers
    customer_cols = ['customer_id', 'city', 'customer_tier']
    curated = valid_orders.merge(
        customers[customer_cols].rename(columns={'city': 'customer_city'}),
        on='customer_id',
        how='left'
    )

    # Join with products
    product_cols = ['product_id', 'name', 'category', 'brand']
    curated = curated.merge(
        products[product_cols].rename(columns={'name': 'product_name'}),
        on='product_id',
        how='left'
    )

    # Compute amounts
    curated['gross_amount'] = curated['quantity'] * curated['unit_price']
    curated['discount_amount'] = curated['gross_amount'] * curated['discount_pct']
    curated['net_amount'] = curated['gross_amount'] - curated['discount_amount']

    # Round to 2 decimal places
    curated['gross_amount'] = curated['gross_amount'].round(2)
    curated['discount_amount'] = curated['discount_amount'].round(2)
    curated['net_amount'] = curated['net_amount'].round(2)

    # Add order_year and order_month
    curated['order_year'] = curated['order_timestamp'].dt.year
    curated['order_month'] = curated['order_timestamp'].dt.month

    # Add audit columns
    curated['processed_at_utc'] = pd.Timestamp.now(tz='UTC')
    curated['pipeline_run_id'] = run_id

    # Rename updated_at to source_updated_at
    curated.rename(columns={'updated_at': 'source_updated_at'}, inplace=True)

    # Compute record_hash
    curated['record_hash'] = curated.apply(
        lambda row: record_hash(row.to_dict(), HASH_KEYS), axis=1
    )

    # Select final columns matching the DB schema
    final_cols = [
        'order_id', 'customer_id', 'product_id', 'order_timestamp',
        'customer_city', 'customer_tier', 'product_name', 'category', 'brand',
        'quantity', 'unit_price', 'discount_pct',
        'gross_amount', 'discount_amount', 'net_amount',
        'status', 'source_updated_at',
        'pipeline_run_id', 'processed_at_utc', 'record_hash',
        'order_year', 'order_month'
    ]
    curated = curated[final_cols]

    # Save to curated directory as Parquet
    curated_dir = path_for('curated_dir')
    curated_dir.mkdir(parents=True, exist_ok=True)
    curated_path = curated_dir / 'sales_order_lines.parquet'
    curated.to_parquet(curated_path, index=False)
    logger.info('[CURATED] Wrote %d curated rows to %s', len(curated), curated_path)

    return curated
