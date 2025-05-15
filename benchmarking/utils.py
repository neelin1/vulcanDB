import pandas as pd
import json

STATS_FILE = "benchmarking/output/stats.csv"


def write_benchmarking_data(data):
    df = pd.DataFrame([data])
    with open(STATS_FILE, "a") as f:
        df.to_csv(f, header=f.tell() == 0, index=False)


def collect_drop_stats(lookup):
    total_dropped = 0
    error_totals = {}
    for info in lookup.values():
        s = info["stats"]
        total_dropped += s["dropped"]
        for err, meta in s["errors"].items():
            error_totals[err] = error_totals.get(err, 0) + meta["count"]

    total_attempts = sum(info["stats"]["attempt"] for info in lookup.values())
    pct_by_reason = {k: 100 * v / total_attempts for k, v in error_totals.items()}
    return {
        "rows_dropped": total_dropped,
        "rows_attempted": total_attempts,
        "dropped_pct": 100 * total_dropped / total_attempts,
        "error_breakdown": json.dumps(pct_by_reason),  # store as JSON string
    }
