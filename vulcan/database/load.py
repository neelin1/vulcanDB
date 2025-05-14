import re
from typing import Dict, List, Any
import pandas as pd
from sqlalchemy import MetaData, Table, UniqueConstraint, select, text, exc
from sqlalchemy.engine import Connection
import logging

logger = logging.getLogger(__name__)


def push_data_in_db(engine, dataframe, table_order, mapping):
    """Improved version with referential integrity and error handling"""
    metadata = MetaData()
    metadata.reflect(bind=engine)
    connection = engine.connect()

    # { "dbColName" -> "csvColName" }
    map_dict = {m.dbColumn: m.csvColumn for m in mapping}

    # table_lookup_info[table_name] = {
    #   "pk_cols": [...],
    #   "unique_cols_in_csv": [...],
    #   "col_map": {...},
    #   "table_obj": ...
    # }
    table_lookup_info = {}

    def get_single_unique_cols(table_obj: Table):
        single_unique_cols = []
        for c in table_obj.columns:
            if c.unique:
                single_unique_cols.append(c)
                continue
            for constraint in table_obj.constraints:
                if (
                    isinstance(constraint, UniqueConstraint)
                    and len(constraint.columns) == 1
                ):
                    uc_col = list(constraint.columns)[0]
                    if uc_col.name == c.name:
                        single_unique_cols.append(c)
                        break
        return single_unique_cols

    for tbl_name in table_order:
        tbl = metadata.tables[tbl_name]
        pk_cols = list(tbl.primary_key.columns)
        single_uniques = get_single_unique_cols(tbl)

        col_map = {}
        for col in tbl.columns:
            # skip autoinc PK from CSV mapping
            if col.autoincrement and col.primary_key:
                continue
            csv_col = map_dict.get(col.name, col.name)
            col_map[col.name] = csv_col

        # Filter single_uniques to those that actually appear in the CSV
        single_uniques_in_csv = []
        for ucol in single_uniques:
            csv_col = map_dict.get(ucol.name, ucol.name)
            if csv_col in dataframe.columns:
                single_uniques_in_csv.append(ucol)

        table_lookup_info[tbl_name] = {
            "table_obj": tbl,
            "pk_cols": pk_cols,
            "unique_cols_in_csv": single_uniques_in_csv,
            "col_map": col_map,
        }

    # Insert row by row. If row has an issue, drop it
    for idx, row in dataframe.iterrows():
        row_data = row.to_dict()
        cleaned_row = None
        max_retries = 2

        for attempt in range(max_retries + 1):
            try:
                trans = connection.begin()

                # We'll do "lookup or insert" for each table in order
                for tbl_name in table_order:
                    info = table_lookup_info[tbl_name]
                    tbl_obj = info["table_obj"]
                    pk_cols = info["pk_cols"]
                    uniq_in_csv = info["unique_cols_in_csv"]
                    col_map = info["col_map"]

                    insert_data = {}

                    for db_col, csv_col in col_map.items():
                        # default from CSV
                        val = (
                            cleaned_row[csv_col]
                            if cleaned_row
                            else row_data.get(csv_col)
                        )
                        insert_data[db_col] = val

                    # ## NEW LOGIC FOR FKS ##
                    # If this table has any foreign key columns referencing another table,
                    # and the CSV does NOT have that ID, we do a "lookup or insert" in the parent
                    # using parent's unique col, then set child col = parent's PK.
                    # This is essential for child referencing "artist_id" when CSV only has "artist_name".
                    for col_obj in tbl_obj.columns:
                        if col_obj.foreign_keys:
                            fk = list(col_obj.foreign_keys)[0]
                            parent_table_name = fk.column.table.name
                            parent_pk_colname = fk.column.name  # e.g. artist_id

                            # If the child col is not in the CSV, or is None,
                            # we try to find the parent's unique col in the CSV
                            # Then we do parent's lookup or insert, and set child col to parent's ID
                            # We do that by:
                            #   1) Check parent's table_lookup_info to see if there's a single-col unique in the CSV
                            #   2) Do "SELECT or INSERT" on parent
                            if insert_data[col_obj.name] is not None:
                                # The CSV might be providing some integer ID, so skip
                                # But typically it's None. We can continue if you want to force parent's PK from CSV
                                continue

                            parent_info = table_lookup_info[parent_table_name]
                            parent_tbl = parent_info["table_obj"]
                            parent_pk_cols = parent_info["pk_cols"]
                            parent_uniqs = parent_info["unique_cols_in_csv"]

                            # We'll do a single-col parent's unique-based lookup
                            # e.g. parent's 'artist_name'
                            # Must find the parent's unique col name in the CSV
                            # Then do "lookup or insert" to get parent's PK, store in child
                            found_parent_val = None
                            parent_matched_by_col = None

                            # If parent PK is in CSV, we can do direct lookup. Usually it's not
                            if len(parent_pk_cols) == 1:
                                ppk = parent_pk_cols[0]
                                ppk_csv = map_dict.get(ppk.name, ppk.name)
                                if ppk_csv in dataframe.columns:
                                    found_parent_val = (
                                        cleaned_row[ppk_csv]
                                        if cleaned_row
                                        else row_data.get(ppk_csv)
                                    )
                                    parent_matched_by_col = ppk

                            # If we didn't find a direct PK, try parent's single-col unique
                            if not found_parent_val and parent_uniqs:
                                for puniq_col in parent_uniqs:
                                    # e.g. "artist_name"
                                    csv_colname = map_dict.get(
                                        puniq_col.name, puniq_col.name
                                    )
                                    v = (
                                        cleaned_row[csv_colname]
                                        if cleaned_row
                                        else row_data.get(csv_colname)
                                    )
                                    if v is not None:
                                        found_parent_val = v
                                        parent_matched_by_col = puniq_col
                                        break

                            if parent_matched_by_col and found_parent_val is not None:
                                # do lookup
                                sel = select(parent_tbl).where(
                                    parent_tbl.c[parent_matched_by_col.name]
                                    == found_parent_val
                                )
                                existing_parent = connection.execute(sel).fetchone()

                                parent_pk_val = None
                                if existing_parent:
                                    # parent row found
                                    if len(parent_pk_cols) == 1:
                                        parent_pk_val = existing_parent[
                                            parent_pk_cols[0].name
                                        ]
                                else:
                                    # insert parent row
                                    parent_insert_data = {}
                                    for pc in parent_tbl.columns:
                                        # skip parent's auto-inc PK from CSV
                                        if pc.autoincrement and pc.primary_key:
                                            continue
                                        csv_c = map_dict.get(pc.name, pc.name)
                                        parent_insert_data[pc.name] = (
                                            cleaned_row[csv_c]
                                            if cleaned_row
                                            else row_data.get(csv_c)
                                        )

                                    res = connection.execute(
                                        parent_tbl.insert(), parent_insert_data
                                    )
                                    if res.inserted_primary_key:
                                        parent_pk_val = res.inserted_primary_key[0]

                                # Now set child's foreign key col to parent's PK
                                if parent_pk_val is not None:
                                    insert_data[col_obj.name] = parent_pk_val

                    # ### Now "lookup or insert" the child row itself, if it has a single-col unique
                    matched_by = None
                    matched_value = None

                    # If child's PK is in CSV, we can do direct PK match
                    if len(pk_cols) == 1:
                        child_pk = pk_cols[0]
                        child_pk_csv = map_dict.get(child_pk.name, child_pk.name)
                        if child_pk_csv in dataframe.columns:
                            matched_by = child_pk.name
                            matched_value = insert_data[child_pk.name]

                    # If no direct PK, see if child has single-col unique in CSV
                    if (not matched_by or matched_value is None) and uniq_in_csv:
                        for ucol in uniq_in_csv:
                            ccsv = map_dict.get(ucol.name, ucol.name)
                            val = insert_data[ucol.name]
                            if val is not None:
                                matched_by = ucol.name
                                matched_value = val
                                break

                    # If we found a way to match an existing row
                    do_insert = True
                    if matched_by and matched_value is not None:
                        col_obj = tbl_obj.c[matched_by]
                        existing_child = connection.execute(
                            select(tbl_obj).where(col_obj == matched_value)
                        ).fetchone()
                        if existing_child:
                            # skip insert
                            do_insert = False

                    # Insert or reuse
                    if do_insert:
                        res_child = connection.execute(tbl_obj.insert(), insert_data)
                    else:
                        # child row already existed
                        pass

                trans.commit()
                break

            except (exc.DataError, exc.IntegrityError) as e:
                trans.rollback()
                if attempt == max_retries:
                    logger.warning(
                        f"Failed row {idx} after {max_retries} attempts: {str(e)}"
                    )
                    break
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

    # Handle certain numeric formatting issues
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
