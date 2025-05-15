# benchmarking/__main__.py
import argparse
import glob
import json
import os
import pandas as pd
from sqlalchemy import text  # For reading table head
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

from vulcan.app import run_pipeline
from vulcan.readers.csv import read_csv
from benchmarking.utils import (
    generate_pie_chart,
    check_table_emptiness,
    generate_summary_report,
    ensure_output_dirs,
    BENCHMARK_OUTPUT_DIR,
)


def run_single_csv_benchmark(csv_path: str, db_uri: str, single_table: bool) -> dict:
    """
    Runs the full pipeline for a single CSV and collects benchmarking results.
    """
    logger.info(f"Starting benchmark for CSV: {csv_path}")
    csv_filename = os.path.basename(csv_path)
    result_for_this_csv = {
        "dataset": csv_filename,
        "status": "INITIATED",  # Initial status
        "tables_info": [],
        "overall_stats_for_this_csv": {},
    }

    try:
        dataframe = read_csv(csv_path)
        result_for_this_csv["dataframe_rows"] = len(dataframe)

        pipeline_output = run_pipeline(dataframe, db_uri, single_table)

        engine = pipeline_output["engine"]
        lookup = pipeline_output["lookup"]
        table_order = pipeline_output["table_order"]

        # Initialize overall stats for this specific CSV
        csv_total_attempted = 0
        csv_total_dropped = 0
        csv_error_breakdown = {}

        for table_name in table_order:
            table_info = {
                "name": table_name,
                "stats": {},
                "chart_path": None,
                "head_data": "N/A",
                "row_count": 0,
            }

            if table_name not in lookup:
                logger.warning(
                    f"Table '{table_name}' not found in lookup for {csv_filename}. Skipping stats."
                )
                table_info["stats"] = {
                    "attempt": 0,
                    "dropped": 0,
                    "errors": {},
                }  # Default empty stats
                # Attempt to check emptiness even if not in lookup, as table might have been created
                table_info["empty_table_failure"] = check_table_emptiness(
                    engine, table_name
                )
                if not table_info["empty_table_failure"]:
                    try:
                        with engine.connect() as connection:  # Fetch head and count if not empty
                            df_table = pd.read_sql_query(
                                f'SELECT * FROM "{table_name}" LIMIT 5;', connection
                            )
                            table_info["head_data"] = df_table.to_string(index=False)
                            count_query = text(f'SELECT COUNT(*) FROM "{table_name}";')
                            table_info["row_count"] = connection.execute(
                                count_query
                            ).scalar_one()
                    except Exception as e:
                        logger.error(
                            f"Error fetching head/count for {table_name} (CSV: {csv_filename}): {e}"
                        )
                result_for_this_csv["tables_info"].append(table_info)
                continue

            stats = lookup[table_name].get(
                "stats", {"attempt": 0, "dropped": 0, "errors": {}}
            )
            table_info["stats"] = stats

            # Aggregate for this CSV's overall stats
            csv_total_attempted += stats.get("attempt", 0)
            csv_total_dropped += stats.get("dropped", 0)
            for reason, details in stats.get("errors", {}).items():
                csv_error_breakdown[reason] = csv_error_breakdown.get(
                    reason, 0
                ) + details.get("count", 0)

            # Generate pie chart for this table
            chart_path = generate_pie_chart(
                table_name, csv_filename, stats, stats.get("errors", {})
            )
            table_info["chart_path"] = chart_path

            # Check for empty table failure
            table_info["empty_table_failure"] = check_table_emptiness(
                engine, table_name
            )

            # Get head and row count for the table from DB
            try:
                with engine.connect() as connection:
                    df_table = pd.read_sql_query(
                        f'SELECT * FROM "{table_name}" LIMIT 5;', connection
                    )  # Get 5 rows for head
                    table_info["head_data"] = df_table.to_string(
                        index=False
                    )  # Convert df head to string

                    count_query = text(f'SELECT COUNT(*) FROM "{table_name}";')
                    table_info["row_count"] = connection.execute(
                        count_query
                    ).scalar_one()
            except Exception as e:
                logger.error(
                    f"Error fetching head/count for {table_name} (CSV: {csv_filename}): {e}"
                )
                table_info["head_data"] = f"Error fetching data: {e}"
                table_info["row_count"] = "Error"
                if not table_info[
                    "empty_table_failure"
                ]:  # If not already marked as empty, consider it a problem
                    table_info["empty_table_failure"] = (
                        True  # If we can't read it, treat as problematic
                    )

            result_for_this_csv["tables_info"].append(table_info)

        # Finalize overall stats for this CSV
        result_for_this_csv["overall_stats_for_this_csv"] = {
            "rows_attempted_across_tables": csv_total_attempted,
            "rows_dropped_across_tables": csv_total_dropped,
            "dropped_pct_across_tables": (
                (csv_total_dropped / csv_total_attempted * 100)
                if csv_total_attempted > 0
                else 0
            ),
            "error_breakdown_across_tables": csv_error_breakdown,
        }
        result_for_this_csv["status"] = "SUCCESS"
        logger.info(f"Successfully processed CSV: {csv_filename}")

    except Exception as e:
        logger.error(
            f"FAILED processing CSV {csv_filename}: {e}", exc_info=True
        )  # Log full traceback
        result_for_this_csv["status"] = "FAILED"
        result_for_this_csv["error_message"] = str(e)

    return result_for_this_csv


def main():
    parser = argparse.ArgumentParser(description="Run Vulcan benchmarking pipeline.")
    parser.add_argument("--db_uri", required=True, help="Database connection URI.")
    parser.add_argument(
        "--csv_dir",
        default="benchmarking/data",
        help="Directory containing CSV files to process.",
    )
    parser.add_argument(
        "--single_table", action="store_true", help="Process CSVs as single tables."
    )
    args = parser.parse_args()

    ensure_output_dirs()

    all_csv_run_results = []

    csv_files_pattern = os.path.join(args.csv_dir, "*.csv")
    csv_files = glob.glob(csv_files_pattern)

    if not csv_files:
        logger.warning(
            f"No CSV files found in {args.csv_dir} matching pattern {csv_files_pattern}. Exiting."
        )
        generate_summary_report([])
        return

    logger.info(f"Found {len(csv_files)} CSV files to process in {args.csv_dir}.")

    for csv_path in csv_files:
        result = run_single_csv_benchmark(csv_path, args.db_uri, args.single_table)
        all_csv_run_results.append(result)

    # After processing all CSVs, generate the summary report
    generate_summary_report(all_csv_run_results)
    logger.info(
        f"Benchmarking complete. Summary report and charts (if any) generated in {BENCHMARK_OUTPUT_DIR}"
    )


if __name__ == "__main__":
    main()
