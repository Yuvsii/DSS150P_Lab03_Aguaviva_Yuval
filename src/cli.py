import argparse
from src.config import PROJECT_ROOT, DB, SETTINGS
from src.common.audit import new_run_id


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
    
    if args.command == 'extract':
        from src.extract.files import extract_sources
        extract_sources(run_id)
    elif args.command == 'transform':
        from src.transform.staging import build_staging
        from src.transform.curated import build_curated
        from src.config import path_for
        staging_dfs = build_staging(path_for('raw_dir'), run_id)
        build_curated(staging_dfs, run_id)
    elif args.command == 'load':
        from src.load.postgres import upsert_curated
        upsert_curated(None, run_id)
    elif args.command == 'validate':
        from src.validate.quality import validate_curated
        validate_curated(None)
    elif args.command == 'benchmark':
        from src.benchmark.storage import run_benchmark
        from src.config import path_for
        run_benchmark(path_for('curated_dir'), path_for('benchmark_dir'), repeats=args.repeats)
    elif args.command == 'load-partition':
        from src.load.postgres import load_partition
        load_partition(None, args.year, args.month, run_id)
    elif args.command == 'run-all':
        from src.extract.files import extract_sources
        from src.transform.staging import build_staging
        from src.transform.curated import build_curated
        from src.load.postgres import upsert_curated
        from src.validate.quality import validate_curated
        from src.config import path_for
        
        extract_sources(run_id)
        staging_dfs = build_staging(path_for('raw_dir'), run_id)
        curated_df = build_curated(staging_dfs, run_id)
        upsert_curated(curated_df, run_id)
        validate_curated(curated_df)
    else:
        raise NotImplementedError(f'Command not implemented: {args.command}')

if __name__ == '__main__':
    main()
