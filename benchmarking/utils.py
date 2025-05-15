# benchmarking/utils.py
import pandas as pd
import json
import matplotlib.pyplot as plt
import os
from sqlalchemy import text  # For checking table row counts
import logging

logger = logging.getLogger(__name__)

# Define output directory for images and text report
BENCHMARK_OUTPUT_DIR = "benchmarking/output"
IMAGES_DIR = os.path.join(BENCHMARK_OUTPUT_DIR, "charts")
SUMMARY_REPORT_FILE = os.path.join(BENCHMARK_OUTPUT_DIR, "summary_report.txt")


def ensure_output_dirs():
    """Ensures that the output directories for charts and reports exist."""
    os.makedirs(IMAGES_DIR, exist_ok=True)
    # BENCHMARK_OUTPUT_DIR will be created if it doesn't exist when writing the report file.


def generate_pie_chart(table_name: str, csv_filename: str, stats: dict, errors: dict):
    """
    Generates and saves a pie chart for drop reasons for a specific table and CSV.
    Returns the path to the saved image, or None if no chart is generated.
    """
    ensure_output_dirs()

    if (
        not errors or not stats or stats.get("dropped", 0) == 0
    ):  # No errors or no drops means no chart
        logger.info(
            f"No chart generated for {table_name} from {csv_filename}: No drops or errors."
        )
        return None

    labels = []
    sizes = []
    for reason, details in errors.items():
        labels.append(f"{reason} ({details['count']})")
        sizes.append(details["count"])

    if not sizes:
        logger.info(
            f"No chart generated for {table_name} from {csv_filename}: No sizes for pie chart."
        )
        return None

    plt.figure(figsize=(12, 8))
    plt.pie(
        sizes,
        labels=labels,
        autopct="%1.1f%%",
        startangle=140,
        textprops={"fontsize": 9},
    )

    chart_title = (
        f"Drop Reasons for Table: {table_name} (CSV: {csv_filename})\n"
        f"Total Dropped: {stats.get('dropped', 0)}/{stats.get('attempt', 0)}"
    )
    plt.title(chart_title, fontsize=12)
    plt.axis("equal")

    safe_csv_filename = "".join(
        c if c.isalnum() else "_" for c in os.path.splitext(csv_filename)[0]
    )
    safe_table_name = "".join(c if c.isalnum() else "_" for c in table_name)

    image_filename = f"drops_{safe_csv_filename}_{safe_table_name}.png"
    image_path = os.path.join(IMAGES_DIR, image_filename)

    try:
        plt.savefig(image_path, bbox_inches="tight")
        logger.info(f"Saved pie chart to {image_path}")
    except Exception as e:
        logger.error(f"Error saving pie chart {image_path}: {e}")
        plt.close()
        return None
    plt.close()
    return image_path


def check_table_emptiness(engine, table_name: str) -> bool:
    """Checks if a table is empty. Returns True if empty, False otherwise."""
    try:
        with engine.connect() as connection:
            query = text(
                f'SELECT COUNT(*) FROM "{table_name}";'
            )  # Ensure table name is quoted
            result = connection.execute(query).scalar_one_or_none()
            return result == 0
    except Exception as e:
        logger.error(f"Error checking emptiness for table {table_name}: {e}")
        return True  # Assume empty or problematic if query fails


def generate_summary_report(all_csv_results: list):
    """
    Generates a text summary report based on the results from all CSVs.
    Writes the report to BENCHMARK_OUTPUT_DIR/summary_report.txt
    """
    ensure_output_dirs()

    overall_total_attempted_records_across_all_csvs = 0
    overall_total_dropped_records_across_all_csvs = 0
    overall_error_counts_aggregated_across_all_csvs = {}  # reason: total_count
    successful_csv_processing_count = 0
    failed_csv_processing_count = 0

    report_lines = []

    report_lines.append(
        "========== Overall Benchmark Summary (Across All CSVs) ==========\n"
    )

    for csv_res in all_csv_results:
        if csv_res["status"] == "SUCCESS":
            successful_csv_processing_count += 1
            if (
                "overall_stats_for_this_csv" in csv_res
                and csv_res["overall_stats_for_this_csv"]
            ):
                csv_overall = csv_res["overall_stats_for_this_csv"]
                overall_total_attempted_records_across_all_csvs += csv_overall.get(
                    "rows_attempted_across_tables", 0
                )
                overall_total_dropped_records_across_all_csvs += csv_overall.get(
                    "rows_dropped_across_tables", 0
                )
                for reason, count in csv_overall.get(
                    "error_breakdown_across_tables", {}
                ).items():
                    overall_error_counts_aggregated_across_all_csvs[reason] = (
                        overall_error_counts_aggregated_across_all_csvs.get(reason, 0)
                        + count
                    )
        else:
            failed_csv_processing_count += 1

    report_lines.append(f"Total CSVs Processed: {len(all_csv_results)}")
    report_lines.append(f"  Successfully Processed: {successful_csv_processing_count}")
    report_lines.append(f"  Failed to Process: {failed_csv_processing_count}\n")

    if overall_total_attempted_records_across_all_csvs > 0:
        overall_drop_percentage_across_all = (
            overall_total_dropped_records_across_all_csvs
            / overall_total_attempted_records_across_all_csvs
        ) * 100
        report_lines.append(
            f"Grand Total Drop Percentage (across all tables in all successful CSVs): {overall_drop_percentage_across_all:.2f}% "
            f"({overall_total_dropped_records_across_all_csvs}/{overall_total_attempted_records_across_all_csvs} records)\n"
        )

        if overall_error_counts_aggregated_across_all_csvs:
            report_lines.append(
                "Grand Total Most Common Drop Reasons (summed across all tables in all successful CSVs):"
            )
            # Sort reasons by count, descending
            sorted_reasons = sorted(
                overall_error_counts_aggregated_across_all_csvs.items(),
                key=lambda item: item[1],
                reverse=True,
            )
            for reason, count in sorted_reasons:
                reason_pct_of_total_drops = (
                    (count / overall_total_dropped_records_across_all_csvs) * 100
                    if overall_total_dropped_records_across_all_csvs > 0
                    else 0
                )
                report_lines.append(
                    f"  - {reason}: {count} occurrences ({reason_pct_of_total_drops:.1f}% of total drops)"
                )
            report_lines.append("\n")
        elif overall_total_dropped_records_across_all_csvs == 0:
            report_lines.append("No records dropped across all successful CSVs.\n")
    elif (
        successful_csv_processing_count > 0
    ):  # Successful CSVs but no records attempted/dropped
        report_lines.append(
            "No records attempted or all attempted records resulted in zero drops across all successful CSVs.\n"
        )
    else:  # No successful CSVs
        report_lines.append(
            "No CSVs processed successfully, or no records attempted in successful CSVs.\n"
        )

    report_lines.append("\n========== Per-CSV Breakdown ==========\n")

    for csv_res in all_csv_results:
        report_lines.append(f"--- CSV File: {csv_res['dataset']} ---\n")
        if csv_res["status"] == "SUCCESS":
            report_lines.append("Status: SUCCESS\n")
            report_lines.append(
                f"  Original rows in CSV: {csv_res.get('dataframe_rows', 'N/A')}\n"
            )

            # Overall stats for this specific CSV
            if (
                "overall_stats_for_this_csv" in csv_res
                and csv_res["overall_stats_for_this_csv"]
            ):
                csv_overall = csv_res["overall_stats_for_this_csv"]
                report_lines.append("  Summary for this CSV (across its tables):")
                report_lines.append(
                    f"    Total records attempted: {csv_overall.get('rows_attempted_across_tables', 0)}"
                )
                report_lines.append(
                    f"    Total records dropped: {csv_overall.get('rows_dropped_across_tables', 0)} "
                    f"({csv_overall.get('dropped_pct_across_tables', 0):.1f}%)"
                )
                if csv_overall.get("error_breakdown_across_tables"):
                    report_lines.append("    Drop reasons for this CSV:")
                    for reason, count in csv_overall[
                        "error_breakdown_across_tables"
                    ].items():
                        report_lines.append(f"      - {reason}: {count}")
                report_lines.append("")  # Newline after CSV summary

            # Per-table information for this CSV
            if "tables_info" in csv_res:
                for table_info in csv_res["tables_info"]:
                    report_lines.append(f"  -- Table: {table_info['name']} --")

                    # Load stats
                    s = table_info.get("stats", {})
                    attempt = s.get("attempt", 0)
                    dropped = s.get("dropped", 0)
                    dropped_pct = (dropped / attempt) * 100 if attempt else 0
                    report_lines.append(
                        f"    Load: dropped {dropped}/{attempt} rows ({dropped_pct:.1f}%)"
                    )

                    if table_info.get("chart_path"):
                        report_lines.append(
                            f"    Drop Reason Chart: {table_info['chart_path']}"
                        )

                    # Detailed drop reasons for this table
                    if s.get("errors"):
                        report_lines.append("    Drop Reasons Breakdown:")
                        for err, meta in s["errors"].items():
                            pct_of_table_attempts = (
                                100 * meta["count"] / attempt if attempt > 0 else 0
                            )
                            report_lines.append(
                                f"      · {err}: {meta['count']} rows ({pct_of_table_attempts:.1f}% of attempts for this table)"
                            )
                            if meta.get(
                                "sample"
                            ):  # Check if sample exists and is not empty
                                for ex_idx, ex in enumerate(meta["sample"]):
                                    report_lines.append(
                                        f"          Sample {ex_idx+1} (CSV row {ex['row_idx']}): {ex['msg'][:150]}..."
                                    )  # Truncate long messages

                    # Data summary
                    report_lines.append(
                        f"    Data after load: {table_info['row_count']} rows"
                    )
                    if table_info.get("empty_table_failure"):
                        report_lines.append(
                            f"    FAILURE: Table is EMPTY after load attempts."
                        )
                    else:
                        report_lines.append(
                            f"    Head of table data:\n{table_info['head_data']}"
                        )
                    report_lines.append("")  # Newline after table section
        else:  # csv_res["status"] == "FAILED"
            report_lines.append("Status: FAILED")
            report_lines.append(
                f"  Error: {csv_res.get('error_message', 'Unknown error')}"
            )
        report_lines.append("\n----------------------------------------\n")

    try:
        with open(SUMMARY_REPORT_FILE, "w") as f:
            f.write("\n".join(report_lines))
        logger.info(f"Summary report written to {SUMMARY_REPORT_FILE}")
    except IOError as e:
        logger.error(f"Failed to write summary report: {e}")
