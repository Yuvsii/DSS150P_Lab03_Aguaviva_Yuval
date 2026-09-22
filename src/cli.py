import argparse
import logging
import pandas as pd
from src.config import PROJECT_ROOT, DB, SETTINGS, path_for
from src.common.audit import new_run_id

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args()

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        return

    # Wire the modular functions together. Keep orchestration logic thin.
    run_id = new_run_id()
    logger.info('Pipeline run_id: %s', run_id)

    if args.command == 'extract':
        try:
            from src.extract.files import extract_sources
            raw_dir = extract_sources(run_id)
            print(f'[EXTRACT] Done. Raw directory: {raw_dir}')
        except Exception as e:
            logger.error('[EXTRACT] Stage failed: %s', e)
            raise

    elif args.command == 'transform':
        try:
            from src.extract.files import extract_sources
            from src.transform.staging import build_staging
            from src.transform.curated import build_curated

            raw_dir = extract_sources(run_id)
            staging_dfs = build_staging(raw_dir, run_id)
            curated_df = build_curated(staging_dfs, run_id)
            print(f'[TRANSFORM] Done. Curated rows: {len(curated_df)}')
        except Exception as e:
            logger.error('[TRANSFORM] Stage failed: %s', e)
            raise

    elif args.command == 'load':
        try:
            from src.load.postgres import upsert_curated
            curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
            if not curated_path.exists():
                raise FileNotFoundError(f'[LOAD] Curated file not found: {curated_path}. Run transform first.')
            curated_df = pd.read_parquet(curated_path)
            count = upsert_curated(curated_df, run_id)
            print(f'[LOAD] Done. Upserted {count} rows.')
        except Exception as e:
            logger.error('[LOAD] Stage failed: %s', e)
            raise

    elif args.command == 'validate':
        try:
            from src.validate.quality import validate_curated
            curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
            if not curated_path.exists():
                raise FileNotFoundError(f'[VALIDATE] Curated file not found: {curated_path}. Run transform first.')
            curated_df = pd.read_parquet(curated_path)
            errors = validate_curated(curated_df)
            if errors:
                print(f'[VALIDATE] {len(errors)} issues found:')
                for e in errors:
                    print(f'  - {e}')
            else:
                print('[VALIDATE] All checks passed.')
        except Exception as e:
            logger.error('[VALIDATE] Stage failed: %s', e)
            raise

    elif args.command == 'benchmark':
        try:
            from src.benchmark.storage import run_benchmark
            run_benchmark(path_for('curated_dir'), path_for('benchmark_dir'), repeats=args.repeats)
        except Exception as e:
            logger.error('[BENCHMARK] Stage failed: %s', e)
            raise

    elif args.command == 'load-partition':
        try:
            from src.load.postgres import load_partition
            curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
            if not curated_path.exists():
                raise FileNotFoundError(f'[LOAD-PARTITION] Curated file not found: {curated_path}. Run transform first.')
            curated_df = pd.read_parquet(curated_path)
            count = load_partition(curated_df, args.year, args.month, run_id)
            print(f'[LOAD-PARTITION] Done. Loaded {count} rows for {args.year}-{args.month:02d}.')
        except Exception as e:
            logger.error('[LOAD-PARTITION] Stage failed: %s', e)
            raise

    elif args.command == 'run-all':
        try:
            from src.extract.files import extract_sources
            logger.info('=== STAGE: EXTRACT ===')
            raw_dir = extract_sources(run_id)
            print(f'[EXTRACT] Done. Raw directory: {raw_dir}')
        except Exception as e:
            logger.error('[EXTRACT] Stage failed: %s', e)
            raise RuntimeError(f'[EXTRACT] Pipeline aborted: {e}') from e

        try:
            from src.transform.staging import build_staging
            logger.info('=== STAGE: STAGING ===')
            staging_dfs = build_staging(raw_dir, run_id)
            for name, df in staging_dfs.items():
                print(f'[STAGING] {name}: {len(df)} rows')
        except Exception as e:
            logger.error('[STAGING] Stage failed: %s', e)
            raise RuntimeError(f'[STAGING] Pipeline aborted: {e}') from e

        try:
            from src.transform.curated import build_curated
            logger.info('=== STAGE: CURATED ===')
            curated_df = build_curated(staging_dfs, run_id)
            print(f'[CURATED] Done. {len(curated_df)} curated rows.')
        except Exception as e:
            logger.error('[CURATED] Stage failed: %s', e)
            raise RuntimeError(f'[CURATED] Pipeline aborted: {e}') from e

        try:
            from src.load.postgres import upsert_curated
            logger.info('=== STAGE: LOAD ===')
            count = upsert_curated(curated_df, run_id)
            print(f'[LOAD] Done. Upserted {count} rows.')
        except Exception as e:
            logger.error('[LOAD] Stage failed: %s', e)
            raise RuntimeError(f'[LOAD] Pipeline aborted: {e}') from e

        try:
            from src.validate.quality import validate_curated
            logger.info('=== STAGE: VALIDATE ===')
            errors = validate_curated(curated_df)
            if errors:
                print(f'[VALIDATE] {len(errors)} issues found:')
                for err in errors:
                    print(f'  - {err}')
            else:
                print('[VALIDATE] All checks passed.')
        except Exception as e:
            logger.error('[VALIDATE] Stage failed: %s', e)
            raise RuntimeError(f'[VALIDATE] Pipeline aborted: {e}') from e

        logger.info('=== PIPELINE COMPLETE ===')

    else:
        raise NotImplementedError(f'Command not implemented: {args.command}')


if __name__ == '__main__':
    main()
