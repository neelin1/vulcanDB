from dotenv import load_dotenv

from vulcan.database.core import populate_database
from vulcan.generators.query import generate_sql_queries
from vulcan.parsers.graph import create_query_dependent_graph, get_table_creation_order

load_dotenv()


def run_pipeline(dataframe, db_uri, db_type):
    response = generate_sql_queries(dataframe, db_type)
    dependent_graph, tables = create_query_dependent_graph(response["queries"])
    print(">> TABLES:", tables)
    print(">> DEPENDENCT GRAPH", dependent_graph)
    table_order = get_table_creation_order(dependent_graph)
    print(">> TABLE ORDER", table_order)
    populate_database(
        db_uri, table_order, tables, dataframe, response.get("alias_mapping", {})
    )
    return response
