from dotenv import load_dotenv
import pandas as pd  # Added for type hinting if not already present elsewhere

from vulcan.generators.query import generate_sql_queries
from vulcan.parsers.graph import create_query_dependent_graph
from vulcan.parsers.dependency import determine_table_creation_order
from vulcan.database.core import initialize_database, execute_queries
from vulcan.database.validator import validate_content
from vulcan.database.load import push_data_in_db


load_dotenv()


def run_pipeline(dataframe: pd.DataFrame, db_uri: str, single_table: bool):
    # Generate Schema, Constraints, and Queries
    # The generate_sql_queries function now handles more, based on query.py changes
    data_dict = generate_sql_queries(dataframe, single_table)

    queries = data_dict["queries"]
    if "table_traits" not in data_dict or "table_list" not in data_dict:
        raise ValueError("table_traits or table_list is missing in data_dict")
    table_traits = data_dict["table_traits"]
    table_list = data_dict["table_list"]

    # Create the dependent graph (still useful for getting the 'tables' dictionary structure)
    dependent_graph, tables_dict_from_graph = create_query_dependent_graph(queries)
    print(">> Dependent Graph:", dependent_graph)
    print(">> Tables Dict from Graph:", tables_dict_from_graph)

    # Determine table creation order
    # This now uses table_traits and table_list as per the notebook
    table_order = determine_table_creation_order(table_traits, table_list)
    print(">> Determined Table Order:", table_order)

    # Initialize the database engine
    engine = initialize_database(db_uri=db_uri)

    # Create tables by executing the CREATE statements in the correct order
    # The 'tables' variable for execute_queries should come from create_query_dependent_graph
    success, error = execute_queries(
        engine, table_order, tables_dict_from_graph
    )  # Ensure 'tables' is correctly sourced
    if not success:
        print(f"Table creation error: {error}")
        # Decide how to handle this error, e.g., raise an exception or return
        raise Exception(f"Table creation failed: {error}")
    else:
        print("Tables created successfully!")

    # Validate schema content
    try:
        validate_content(engine, dataframe, table_order, table_traits, single_table)
        print("Schema validation passed!")
    except ValueError as e:
        print(f"Schema validation failed: {e}")
        raise e  # Re-raise the exception to halt pipeline if validation fails

    # Populate Tables with CSV Data
    lookup = push_data_in_db(engine, dataframe, table_order, table_traits)
    print("Data insertion complete!")

    total_rows = len(dataframe)

    # Count constraints from queries
    total_constraints_count = 0
    constraint_keywords = ["PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK"]
    if queries:
        for query_string in queries:  # queries is expected to be a list of SQL strings
            if isinstance(query_string, str):
                for keyword in constraint_keywords:
                    total_constraints_count += query_string.upper().count(
                        keyword.upper()
                    )

    for tbl_name in table_order:
        s = lookup.get(tbl_name, {}).get(
            "stats", {"attempt": 0, "dropped": 0}
        )  # Defensive get
        attempt = s.get("attempt", 0)
        dropped = s.get("dropped", 0)
        dropped_pct = (dropped / attempt) * 100 if attempt else 0
        print(
            f"{tbl_name}: dropped {dropped}/{attempt} rows ({dropped_pct:.1f}%) during load"
        )

    return {
        "engine": engine,
        "lookup": lookup,
        "table_order": table_order,
        "table_traits": table_traits,  # Pass along for consistency if needed
        "dataframe_rows": total_rows,  # Original number of rows in the input dataframe
        "total_constraints_count": total_constraints_count,  # Add the count here
    }
