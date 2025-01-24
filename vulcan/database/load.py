from typing import Dict
import pandas as pd
from sqlalchemy import MetaData


def push_data_in_db(engine, dataframe, table_order, mapping):
    # Reflect the database schema
    metadata = MetaData()
    metadata.reflect(bind=engine)

    minimal_map_dict = {m.dbColumn: m.csvColumn for m in mapping}

    # Process and insert data for each table
    for table_name in table_order:
        table = metadata.tables.get(table_name)
        if table is None:
            print(f"Table {table_name} does not exist in the database.")
            continue

        # build mapping from table column names to DataFrame column names
        table_to_df_col_mapping = {}
        for col in table.columns:
            if col.primary_key and col.autoincrement:
                continue

            if col.name in minimal_map_dict:
                # This is a changed column name
                df_col_name = minimal_map_dict[col.name]
            else:
                # No entry => same name in CSV
                df_col_name = col.name

            if df_col_name not in dataframe.columns:
                print(f"Error: Column {df_col_name} not found in DataFrame.")
                continue

            table_to_df_col_mapping[col.name] = df_col_name
        print(f">> TABLE TO DF COLUMN MAPPING: {table_to_df_col_mapping}")

        df_filtered = dataframe[list(table_to_df_col_mapping.values())].copy()
        df_filtered.rename(
            columns={v: k for k, v in table_to_df_col_mapping.items()}, inplace=True
        )

        # Insert
        # df_filtered.to_sql(table_name, con=engine, if_exists="append", index=False)
        # print(f"Data successfully inserted into {table_name}.")
        for idx, row in df_filtered.iterrows():
            try:
                row_df = pd.DataFrame([row])
                row_df.to_sql(table_name, con=engine, if_exists="append", index=False)
            except Exception as e:
                # Catch specific errors (e.g., invalid input syntax)
                if "invalid input syntax for type integer" in str(e):
                    print(
                        f"Error inserting row {idx} into {table_name}: {e}. Cleaning data and retrying."
                    )

                    # Attempt to clean the row data (e.g., remove commas from numbers)
                    cleaned_row = row.apply(
                        lambda x: (
                            str(x).replace(",", "")
                            if isinstance(x, str) and "," in x
                            else x
                        )
                    )

                    try:
                        # Retry insertion with cleaned data
                        cleaned_row_df = pd.DataFrame([cleaned_row])
                        cleaned_row_df.to_sql(
                            table_name, con=engine, if_exists="append", index=False
                        )
                        print(f"Row {idx} successfully inserted after cleaning.")
                    except Exception as retry_error:
                        print(
                            f"Retry failed for row {idx} in {table_name}: {retry_error}. Skipping row."
                        )
                else:
                    # Log and skip other types of errors
                    print(f"Unexpected error for row {idx}: {e}. Skipping row.")

        print(f"Data insertion complete for {table_name}.")
