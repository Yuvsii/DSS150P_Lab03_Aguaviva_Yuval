import logging
import json
import pandas as pd
from pathlib import Path
from src.config import path_for, SETTINGS
from src.common.audit import utc_now_iso

logger = logging.getLogger(__name__)

ALLOWED_STATUSES = SETTINGS['quality']['allowed_order_statuses']
MIN_QTY = SETTINGS['quality']['min_quantity']
MAX_QTY = SETTINGS['quality']['max_quantity']


def _dedup(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Deduplicate by business key, keeping the row with the greatest updated_at."""
    before = len(df)
    df = df.sort_values('updated_at', ascending=False).drop_duplicates(subset=[key], keep='first')
    after = len(df)
    if before != after:
        logger.info('[STAGING] Deduplicated %s: %d -> %d rows', key, before, after)
    return df


def _stage_customers(raw_dir: Path, run_id: str):
    """Stage customers: dedup, normalize, quarantine missing emails."""
    df = pd.read_csv(raw_dir / 'customers.csv')
    logger.info('[STAGING] Read %d raw customers', len(df))

    # Parse timestamps as UTC
    df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True)

    # Deduplicate
    df = _dedup(df, 'customer_id')

    # Normalize email to lowercase
    df['email'] = df['email'].str.strip().str.lower()

    # Normalize city: strip whitespace and title-case
    df['city'] = df['city'].str.strip().str.title()

    # Quarantine missing emails? No, the instructions say:
    # "retain missing email as a visible quality condition"
    # So we do NOT filter them out or quarantine them.
    valid = df.copy()
    quarantine = pd.DataFrame(columns=df.columns.tolist() + ['quarantine_reason'])

    # Add audit columns
    valid['pipeline_run_id'] = run_id
    valid['staged_at_utc'] = utc_now_iso()

    return valid, quarantine


def _stage_products(raw_dir: Path, run_id: str):
    """Stage products: flatten category, dedup, quarantine invalid prices."""
    with open(raw_dir / 'products.json', encoding='utf-8') as f:
        products_raw = json.load(f)

    df = pd.DataFrame(products_raw)
    logger.info('[STAGING] Read %d raw products', len(df))

    # Flatten nested category
    df['category_name'] = df['category'].apply(lambda c: c.get('name', '') if isinstance(c, dict) else '')
    df['department'] = df['category'].apply(lambda c: c.get('department', '') if isinstance(c, dict) else '')
    df.drop(columns=['category'], inplace=True)
    df.rename(columns={'category_name': 'category'}, inplace=True)

    # Parse timestamps as UTC
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True)

    # Deduplicate
    df = _dedup(df, 'product_id')

    # Parse price numeric
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')

    # Quarantine invalid prices (null, <= 0)
    invalid_price = df['unit_price'].isna() | (df['unit_price'] <= 0)
    quarantine = df[invalid_price].copy()
    quarantine['quarantine_reason'] = 'invalid_unit_price'
    valid = df[~invalid_price].copy()

    if len(quarantine) > 0:
        logger.info('[STAGING] Quarantined %d products (invalid_unit_price)', len(quarantine))

    # Add audit columns
    valid['pipeline_run_id'] = run_id
    valid['staged_at_utc'] = utc_now_iso()

    return valid, quarantine


def _stage_orders(raw_dir: Path, run_id: str):
    """Stage orders: dedup, validate quantity/status, quarantine invalid."""
    df = pd.read_csv(raw_dir / 'orders.csv')
    logger.info('[STAGING] Read %d raw orders', len(df))

    # Parse timestamps as UTC
    df['order_timestamp'] = pd.to_datetime(df['order_timestamp'], utc=True)
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True)

    # Deduplicate
    df = _dedup(df, 'order_id')

    # Validate quantity
    qty_invalid = (df['quantity'] < MIN_QTY) | (df['quantity'] > MAX_QTY)

    # Validate status
    status_invalid = ~df['status'].isin(ALLOWED_STATUSES)

    # Build quarantine with reasons
    quarantine_frames = []

    qty_quarantine = df[qty_invalid].copy()
    if len(qty_quarantine) > 0:
        qty_quarantine['quarantine_reason'] = 'quantity_out_of_range'
        quarantine_frames.append(qty_quarantine)
        logger.info('[STAGING] Quarantined %d orders (quantity_out_of_range)', len(qty_quarantine))

    status_quarantine = df[status_invalid & ~qty_invalid].copy()
    if len(status_quarantine) > 0:
        status_quarantine['quarantine_reason'] = 'invalid_status'
        quarantine_frames.append(status_quarantine)
        logger.info('[STAGING] Quarantined %d orders (invalid_status)', len(status_quarantine))

    all_invalid = qty_invalid | status_invalid
    valid = df[~all_invalid].copy()

    quarantine = pd.concat(quarantine_frames, ignore_index=True) if quarantine_frames else pd.DataFrame()

    # Add audit columns
    valid['pipeline_run_id'] = run_id
    valid['staged_at_utc'] = utc_now_iso()

    return valid, quarantine


def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets.

    Returns a dict of staging DataFrames and writes quarantine records to disk.
    """
    raw_dir = Path(raw_dir)

    # Find the latest run directory if raw_dir itself doesn't contain source files
    run_dirs = sorted(raw_dir.glob('run_id=*'))
    if run_dirs:
        raw_dir = run_dirs[-1]
        logger.info('[STAGING] Using raw directory: %s', raw_dir)

    customers, q_customers = _stage_customers(raw_dir, run_id)
    products, q_products = _stage_products(raw_dir, run_id)
    orders, q_orders = _stage_orders(raw_dir, run_id)

    # Combine all quarantine records
    quarantine_frames = []
    for q_df, source in [(q_customers, 'customers'), (q_products, 'products'), (q_orders, 'orders')]:
        if len(q_df) > 0:
            q_df = q_df.copy()
            q_df['source_entity'] = source
            quarantine_frames.append(q_df)

    quarantine_dir = path_for('quarantine_dir')
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    if quarantine_frames:
        quarantine_df = pd.concat(quarantine_frames, ignore_index=True)
        quarantine_path = quarantine_dir / f'quarantine_{run_id}.csv'
        quarantine_df.to_csv(quarantine_path, index=False)
        logger.info('[STAGING] Wrote %d quarantine records to %s', len(quarantine_df), quarantine_path)
    else:
        quarantine_df = pd.DataFrame()

    # Write staging outputs as Parquet
    staging_dir = path_for('staging_dir')
    staging_dir.mkdir(parents=True, exist_ok=True)
    customers.to_parquet(staging_dir / 'customers.parquet', index=False)
    products.to_parquet(staging_dir / 'products.parquet', index=False)
    orders.to_parquet(staging_dir / 'orders.parquet', index=False)
    logger.info('[STAGING] Wrote staging parquet files to %s', staging_dir)

    staging = {
        'customers': customers,
        'products': products,
        'orders': orders,
    }

    logger.info('[STAGING] Staging complete: %d customers, %d products, %d orders, %d quarantined',
                len(customers), len(products), len(orders),
                len(quarantine_df) if not quarantine_df.empty else 0)

    return staging
