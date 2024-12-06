from mo_sql_parsing import parse
from pglast import parse_sql
from pglast.enums import ConstrType
from pglast.ast import CreateStmt, ColumnDef, Constraint, RawStmt
from typing import Any, Dict, List


# TODO: Make sure references work with pg_last
def extract_columns_from_parsed_query(parsed_query: List[RawStmt]) -> List[ColumnDef]:
    """
    Extracts the column definitions from a parsed SQL query.

    Args:
        parsed_query (List[RawStmt]): The parsed SQL statements.

    Returns:
        List[ColumnDef]: A list of column definitions.
    """
    create_stmt = parsed_query[0].stmt
    if not isinstance(create_stmt, CreateStmt):
        raise ValueError("Query is not a CREATE TABLE statement")
    columns = []
    for column_def in create_stmt.tableElts:  # type: ignore
        if isinstance(column_def, ColumnDef):
            columns.append(column_def)
    return columns


# TODO: Make sure references work with pg_last
def extract_table_constraints_from_parsed_query(
    parsed_query: List[RawStmt],
) -> List[Constraint]:
    """
    Extracts table-level constraints from a parsed SQL query.

    Args:
        parsed_query (List[RawStmt]): The parsed SQL statements.

    Returns:
        List[Constraint]: A list of table-level constraints.
    """
    create_stmt = parsed_query[0].stmt
    if not isinstance(create_stmt, CreateStmt):
        raise ValueError("Query is not a CREATE TABLE statement")
    constraints = []
    for constraint in create_stmt.tableElts:  # type: ignore
        if isinstance(constraint, Constraint):
            constraints.append(constraint)
    return constraints


def extract_column_names_from_parsed_query(parsed_query: List[RawStmt]) -> List[str]:
    """
    Extracts column names from the parsed SQL query.

    Args:
        parsed_query (dict): A parsed SQL query dictionary.

    Returns:
        list: A list of column names as strings.
    """
    create_stmt = parsed_query[0].stmt
    if not isinstance(create_stmt, CreateStmt):
        raise ValueError("Query is not a CREATE TABLE statement")
    columns = []
    for column_def in create_stmt.tableElts:  # type: ignore
        if isinstance(column_def, ColumnDef):
            columns.append(column_def.colname)
    return columns


def extract_foreign_keys_from_parsed_query(parsed_query: List[RawStmt]) -> List[str]:
    """
    Extracts all foreign key references from the parsed SQL query.

    Combines references defined in both column definitions and table constraints.

    Args:
        parsed_query (dict): A parsed SQL query dictionary.

    Returns:
        list: A list of unique table names referenced by foreign key constraints.
    """
    create_stmt = parsed_query[0].stmt
    if not isinstance(create_stmt, CreateStmt):
        raise ValueError("Query is not a CREATE TABLE statement")
    foreign_tables = set()

    # Column-level constraints
    for column_def in create_stmt.tableElts:  # type: ignore
        if isinstance(column_def, ColumnDef):
            for constraint in column_def.constraints or []:
                if (
                    isinstance(constraint, Constraint)
                    and constraint.contype == ConstrType.CONSTR_FOREIGN
                ):
                    fk_table = constraint.pktable.relname  # type: ignore
                    foreign_tables.add(fk_table)

    # Table-level constraints
    for constraint in create_stmt.tableElts:  # type: ignore
        if (
            isinstance(constraint, Constraint)
            and constraint.contype == ConstrType.CONSTR_FOREIGN
        ):
            fk_table = constraint.pktable.relname  # type: ignore
            foreign_tables.add(fk_table)

    return list(foreign_tables)


def parse_sql_query(query: str) -> Dict[str, Any]:
    """
    Parses a SQL `CREATE TABLE` query and extracts table metadata.

    Args:
        query (str): A SQL `CREATE TABLE` statement.

    Returns:
        dict: A dictionary containing:
            - "query": The original SQL query string.
            - "name": The string name of the table.
            - "columns": A list of column names in the table.
            - "foreign_keys": A list of tables referenced by foreign key constraints.
    """
    parsed_statements = parse_sql(query)
    create_stmt = parsed_statements[0].stmt

    if not isinstance(create_stmt, CreateStmt):
        raise ValueError("Query is not a CREATE TABLE statement")

    table_name = create_stmt.relation.relname  # type: ignore

    return {
        "query": query,
        "name": table_name,
        "columns": extract_column_names_from_parsed_query(parsed_statements),
        "foreign_keys": extract_foreign_keys_from_parsed_query(parsed_statements),
    }
