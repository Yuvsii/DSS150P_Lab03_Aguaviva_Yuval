import logging
from pathlib import Path
import shutil
from src.config import path_for

logger = logging.getLogger(__name__)


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory.

    Creates data/raw/run_id=<run_id>/ and copies all source files there.
    Source files are never modified in place.
    """
    source_dir = path_for('source_dir')
    raw_dir = path_for('raw_dir') / f'run_id={run_id}'

    if raw_dir.exists():
        logger.warning('[EXTRACT] Raw directory already exists, removing: %s', raw_dir)
        shutil.rmtree(raw_dir)

    raw_dir.mkdir(parents=True, exist_ok=True)
    logger.info('[EXTRACT] Created raw directory: %s', raw_dir)

    expected_files = ['customers.csv', 'products.json', 'orders.csv']
    for filename in expected_files:
        src = source_dir / filename
        if not src.exists():
            raise FileNotFoundError(f'[EXTRACT] Source file not found: {src}')
        dst = raw_dir / filename
        shutil.copy2(src, dst)
        logger.info('[EXTRACT] Copied %s -> %s', src, dst)

    logger.info('[EXTRACT] Extraction complete. %d files copied.', len(expected_files))
    return raw_dir
