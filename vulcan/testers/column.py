import pandas as pd
from sqlglot import parse_one

import vulcan.parsers.query as vpq


def get_missing_columns(queries: list[str], df: pd.DataFrame):
    generated_columns = set()
    for query in queries:
        parsed_query = parse_one(query, read="postgres")
        extracted_columns = vpq.extract_column_names_from_parsed_query(parsed_query)
        generated_columns.update(extracted_columns)

    original_columns = set(df.columns)
    return original_columns - generated_columns
