from typing import Dict
from sqlalchemy import MetaData


def push_data_in_db(engine, dataframe, table_order, alias_mapping):
    # Reflect the database schema
    metadata = MetaData()
    metadata.reflect(bind=engine)
    # Process and insert data for each table
    for table_name in table_order:
        table = metadata.tables.get(table_name)
        if table is None:
            print(f"Table {table_name} does not exist in the database.")
            continue

        # build mapping from table column names to DataFrame column names
        table_to_df_col_mapping = {}
        print(">> COLUMNS", table.columns)
        for col in table.columns:
            print("COL:", col)
            if col.primary_key and col.autoincrement:  # skip auto-increment columns
                print("ZIG 1")
                continue
            if col.name in alias_mapping:
                print("ZIG 2")
                df_col_name = alias_mapping[col.name]
            elif col.name in dataframe.columns:
                print("ZIG 3")
                df_col_name = col.name
            else:
                print("ZIG 4")
                print(
                    f"Error: Column {col.name} not found in alias mapping or DataFrame columns."
                )
                continue
            table_to_df_col_mapping[col.name] = df_col_name

        print(f">> TABLE TO DF COLUMN MAPPING: {table_to_df_col_mapping}")

        df_filtered = dataframe[list(table_to_df_col_mapping.values())].copy()

        df_filtered.rename(
            columns={v: k for k, v in table_to_df_col_mapping.items()}, inplace=True
        )
        for col in table.columns:
            if (
                col.autoincrement
                and col.autoincrement != "auto"
                and col.name in df_filtered.columns
            ):
                df_filtered.drop(columns=[col.name], inplace=True, errors="ignore")

        print(">> DF FILTERED", df_filtered.columns)
        # Insert the filtered DataFrame into the database
        df_filtered.to_sql(table_name, con=engine, if_exists="append", index=False)
        print(f"Data successfully inserted into {table_name}.")
