import logging
from src.config import SETTINGS

logger = logging.getLogger(__name__)

ALLOWED_STATUSES = SETTINGS['quality']['allowed_order_statuses']
MIN_QTY = SETTINGS['quality']['min_quantity']
MAX_QTY = SETTINGS['quality']['max_quantity']


def validate_curated(df) -> list[str]:
    """Return a list of human-readable validation errors.

    Minimum checks: order_id uniqueness/non-null, quantity range,
    nonnegative amounts, allowed statuses, required audit fields.
    """
    errors = []

    if df is None or df.empty:
        errors.append('Curated DataFrame is empty or None')
        return errors

    # 1. order_id non-null
    null_order_ids = df['order_id'].isna().sum()
    if null_order_ids > 0:
        errors.append(f'Found {null_order_ids} null order_id values')

    # 2. order_id uniqueness
    dup_order_ids = df['order_id'].duplicated().sum()
    if dup_order_ids > 0:
        errors.append(f'Found {dup_order_ids} duplicate order_id values')

    # 3. quantity range
    qty_out = ((df['quantity'] < MIN_QTY) | (df['quantity'] > MAX_QTY)).sum()
    if qty_out > 0:
        errors.append(f'Found {qty_out} rows with quantity outside [{MIN_QTY}, {MAX_QTY}]')

    # 4. nonnegative amounts
    for col in ['gross_amount', 'discount_amount', 'net_amount']:
        if col in df.columns:
            neg = (df[col] < 0).sum()
            if neg > 0:
                errors.append(f'Found {neg} rows with negative {col}')

    # 5. allowed statuses
    invalid_statuses = ~df['status'].isin(ALLOWED_STATUSES)
    if invalid_statuses.sum() > 0:
        bad = df.loc[invalid_statuses, 'status'].unique().tolist()
        errors.append(f'Found {invalid_statuses.sum()} rows with invalid status: {bad}')

    # 6. required audit fields
    for col in ['pipeline_run_id', 'processed_at_utc', 'record_hash']:
        if col not in df.columns:
            errors.append(f'Missing required audit column: {col}')
        elif df[col].isna().any():
            n = df[col].isna().sum()
            errors.append(f'Found {n} null values in audit column: {col}')

    if errors:
        logger.warning('[VALIDATE] %d validation issues found', len(errors))
        for e in errors:
            logger.warning('[VALIDATE]   - %s', e)
    else:
        logger.info('[VALIDATE] All validation checks passed (%d rows)', len(df))

    return errors
