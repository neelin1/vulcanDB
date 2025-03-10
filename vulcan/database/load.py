import re
from typing import Dict, List, Any
import pandas as pd
from sqlalchemy import MetaData, Table, select, text, exc
from sqlalchemy.engine import Connection
import logging

logger = logging.getLogger(__name__)


def push_data_in_db(engine, dataframe, table_order, mapping):
    """Improved version with referential integrity and error handling"""
    metadata = MetaData()
    metadata.reflect(bind=engine)
    connection = engine.connect()
    map_dict = {m.dbColumn: m.csvColumn for m in mapping}

    # Precompute column mappings for all tables
    table_mappings = {}
    for table_name in table_order:
        table = metadata.tables[table_name]
        col_map = {}
        for col in table.columns:
            if col.primary_key and col.autoincrement:
                continue
            csv_col = map_dict.get(col.name, col.name)
            col_map[col.name] = csv_col
        table_mappings[table_name] = col_map

    # Process rows with transaction-per-row and error handling
    for idx, row in dataframe.iterrows():
        row_data = row.to_dict()
        max_retries = 2
        cleaned_row = None

        for attempt in range(max_retries + 1):
            try:
                trans = connection.begin()
                pk_cache = {}

                # Process all tables for this row
                for table_name in table_order:
                    table = metadata.tables[table_name]
                    col_map = table_mappings[table_name]

                    # Build insert data with cleaned values
                    insert_data = {}
                    for db_col, csv_col in col_map.items():
                        value = (
                            cleaned_row[csv_col]
                            if cleaned_row
                            else row_data.get(csv_col)
                        )

                        # Handle foreign key references
                        col_obj = table.columns[db_col]
                        if col_obj.foreign_keys:
                            fk = next(iter(col_obj.foreign_keys))
                            ref_table_name = fk.column.table.name
                            ref_col_name = fk.column.name

                            # Get referenced value from original data
                            ref_csv_col = map_dict.get(ref_col_name, ref_col_name)
                            ref_value = row_data.get(ref_csv_col)

                            # Lookup in cache or database
                            if ref_value in pk_cache.get(ref_table_name, {}):
                                insert_data[db_col] = pk_cache[ref_table_name][
                                    ref_value
                                ]
                            else:
                                # Find existing reference
                                stmt = select([fk.column]).where(fk.column == ref_value)
                                result = connection.execute(stmt).fetchone()
                                if result:
                                    pk_cache.setdefault(ref_table_name, {})[
                                        ref_value
                                    ] = result[0]
                                    insert_data[db_col] = result[0]

                        # Add direct value (if not already set by FK)
                        if db_col not in insert_data:
                            insert_data[db_col] = value

                    # Check for existing entries using natural key
                    natural_key = [c.name for c in table.primary_key]
                    if natural_key:
                        # Build where clauses using column objects
                        where_clauses = [
                            table.columns[col] == insert_data[col]
                            for col in natural_key
                        ]

                        # Proper select construction
                        stmt = select(table).where(*where_clauses)
                        existing = connection.execute(stmt).fetchone()

                        if existing:
                            # Cache primary key for downstream references
                            pk = existing[0]  # Assuming single-column PK
                            if natural_key[0] in insert_data:
                                natural_value = insert_data[natural_key[0]]
                                pk_cache.setdefault(table_name, {})[natural_value] = pk
                            continue

                    # Insert new record
                    result = connection.execute(table.insert(), insert_data)
                    if result.inserted_primary_key:
                        pk = result.inserted_primary_key[0]
                        if natural_key and natural_key[0] in insert_data:
                            natural_value = insert_data[natural_key[0]]
                            pk_cache.setdefault(table_name, {})[natural_value] = pk

                trans.commit()
                break  # Success - move to next row

            except (exc.DataError, exc.IntegrityError) as e:
                trans.rollback()
                if attempt == max_retries:
                    logger.warning(
                        f"Failed row {idx} after {max_retries} attempts: {str(e)}"
                    )
                    break

                # Clean problematic data
                cleaned_row = clean_row_data(
                    row_data if attempt == 0 else cleaned_row, str(e)
                )
                logger.info(
                    f"Retrying row {idx} (attempt {attempt + 1}) with cleaned data"
                )

            except Exception as e:
                trans.rollback()
                logger.error(f"Unexpected error processing row {idx}: {str(e)}")
                break


def clean_row_data(row_data: Dict[str, Any], error_msg: str) -> Dict[str, Any]:
    """Attempt to clean data based on error message"""
    cleaned = row_data.copy()

    # Handle numeric formatting issues
    if "invalid input syntax" in error_msg and "type numeric" in error_msg:
        for key, value in row_data.items():
            if isinstance(value, str):
                cleaned[key] = value.replace(",", "").strip()

    # Handle string truncation
    if "value too long" in error_msg:
        col_match = re.search(r"column \"(\w+)\"", error_msg)
        if col_match:
            col_name = col_match.group(1)
            if col_name in cleaned:
                cleaned[col_name] = str(cleaned[col_name])[:255]

    return cleaned
