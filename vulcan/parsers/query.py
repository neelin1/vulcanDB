from sqlglot import parse_one
from sqlglot.expressions import Create, Table


def extract_columns_from_parsed_query(parsed_query):
    if "columns" in parsed_query:
        if isinstance(parsed_query["columns"], list):
            return parsed_query["columns"]
        return [parsed_query["columns"]]
    return []


def extract_table_constraints_from_parsed_query(parsed_query):
    if "constraint" in parsed_query:
        if isinstance(parsed_query["constraint"], list):
            return parsed_query["constraint"]
        return [parsed_query["constraint"]]
    return []


def extract_column_names_from_parsed_query(parsed_query):
    columns = []
    for column in parsed_query.expressions:
        if hasattr(column, "this") and hasattr(column.this, "this"):
            columns.append(column.this.this.name)
    return columns


def extract_references_from_columns(parsed_query):
    foreign_tables = []
    parsed_columns = extract_columns_from_parsed_query(parsed_query)
    for column in parsed_columns:
        if "references" in column:
            foreign_tables.append(column["references"]["table"])
    return foreign_tables


def extract_references_from_table(parsed_query):
    table_constraints = extract_table_constraints_from_parsed_query(parsed_query)
    foreign_tables = []
    for constraint in table_constraints:
        if "foreign_key" in constraint:
            fk_table = constraint["foreign_key"]["references"]
            foreign_tables.append(fk_table["table"])
    return foreign_tables


def extract_foreign_keys_from_parsed_query(parsed_query):
    foreign_tables = set()
    # foreign keys from column constraints
    for column in parsed_query.expressions:
        for constraint in column.constraints or []:
            if constraint.kind == "FOREIGN_KEY":
                foreign_table = constraint.expression.this.this.name
                foreign_tables.add(foreign_table)
    # foreign keys from table constraints
    for constraint in parsed_query.constraints or []:
        if constraint.kind == "FOREIGN_KEY":
            foreign_table = constraint.expression.this.this.name
            foreign_tables.add(foreign_table)
    return list(foreign_tables)


def parse_sql_query(query: str):
    parsed_query = parse_one(query, read="postgres")
    if not isinstance(parsed_query, Create):
        raise ValueError("Query is not a CREATE TABLE statement")

    print(">> TABLE NAME: ", parsed_query.this.this.name)
    return {
        "query": query,
        "name": parsed_query.this.this.name,
        "columns": extract_column_names_from_parsed_query(parsed_query),
        "foreign_keys": extract_foreign_keys_from_parsed_query(parsed_query),
    }
