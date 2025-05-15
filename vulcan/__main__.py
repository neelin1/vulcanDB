import argparse

from vulcan.app import run_pipeline
from vulcan.readers.csv import read_csv


def main():
    print("Running Main Vulcan")
    parser = argparse.ArgumentParser(description="Process some arguments.")
    parser.add_argument(
        "-f", "--file_name", type=str, help="File name containing SQL queries"
    )
    parser.add_argument(
        "--db_uri", type=str, help="Path to the database file", default=None
    )
    parser.add_argument(
        "--single_table",
        action="store_true",
        help="Force generation of a single table schema (default: attempts multi-table)",
    )

    args = parser.parse_args()
    if not args.db_uri:
        raise ValueError(
            "You must provide a valid --db_uri for the PostgreSQL database."
        )

    dataframe = read_csv(args.file_name)
    run_pipeline(dataframe, args.db_uri, args.single_table)


if __name__ == "__main__":
    main()
