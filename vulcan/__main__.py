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
        "--db_type",
        type=str,
        choices=["postgres", "sqlite"],
        help="Type of the database",
        default="postgres",
    )

    args = parser.parse_args()
    if args.db_type == "postgres" and not args.db_uri:
        raise ValueError(
            "You must provide a valid --db_uri when using --db_type postgres."
        )

    dataframe = read_csv(args.file_name)
    run_pipeline(dataframe, args.db_uri, args.db_type)


if __name__ == "__main__":
    main()
