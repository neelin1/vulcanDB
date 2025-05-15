import argparse, glob, json, os, pandas as pd
from vulcan.app import run_pipeline
from vulcan.readers.csv import read_csv
from vulcan.database.load import push_data_in_db
from benchmarking.utils import write_benchmarking_data, collect_drop_stats


def run_single_benchmark(csv_path, db_uri, single_table):
    df = read_csv(csv_path)
    try:
        data = run_pipeline(df, db_uri, single_table)
        engine = data["engine"]  # if you return it; else re-initialise
        lookup = push_data_in_db(engine, df, data["table_order"], data["table_traits"])

        drop_stats = collect_drop_stats(lookup)

        record = {
            "dataset": os.path.basename(csv_path),
            "status": "SUCCESS",
            "rows_total": len(df),
            **drop_stats,  # flattened dict
        }
    except Exception as e:
        record = {
            "dataset": os.path.basename(csv_path),
            "status": "FAILED",
            "error_message": str(e),
        }
    write_benchmarking_data(record)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db_uri", required=True)
    p.add_argument("--single_table", action="store_true")
    args = p.parse_args()

    for csv in glob.glob("benchmarking/data/*.csv"):
        print(f"→ benchmarking {csv}")
        run_single_benchmark(csv, args.db_uri, args.single_table)


if __name__ == "__main__":
    main()
