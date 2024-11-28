from sqlglot import parse_one
from sqlglot.expressions import ColumnDef, ForeignKey, Constraint, Create

import vulcan.parsers.query as vpq


def get_column_constraints(parsed_query, constraint_count):
    for column in parsed_query.expressions:
        if isinstance(column, ColumnDef):
            # Check for primary key, unique, not null, and default within column definition
            for constraint in column.constraints or []:
                if constraint.kind == "PRIMARY_KEY":
                    constraint_count["primary_key"] += 1
                if constraint.kind == "NOT_NULL":
                    constraint_count["not_null"] += 1
                if constraint.kind == "UNIQUE":
                    constraint_count["unique"] += 1
                if constraint.kind == "DEFAULT":
                    constraint_count["default"] += 1
                if constraint.kind == "CHECK":
                    constraint_count["check"] += 1
        elif isinstance(column, Constraint):
            # TODO: make sure this works
            if column.kind == "FOREIGN_KEY":
                constraint_count["foreign_key"] += 1
    return constraint_count


def get_table_constraints(parsed_query, constraint_count):
    for column in parsed_query.expressions:
        for constraint in column.constraints or []:
            if constraint.kind == "FOREIGN_KEY":
                constraint_count["foreign_key"] += 1
            if constraint.kind == "CHECK":
                constraint_count["check"] += 1
    return constraint_count


def count_constraints(sql_query):
    # Initialize the counter for constraints
    constraint_count = {
        "primary_key": 0,
        "foreign_key": 0,
        "unique": 0,
        "check": 0,
        "not_null": 0,
        "default": 0,
    }

    # Parse the SQL query into an AST
    parsed_query = parse_one(sql_query, read="postgres")
    if not isinstance(parsed_query, Create):
        raise ValueError("Query is not a CREATE TABLE statement")

    # Process column level constraints:
    constraint_count = get_column_constraints(parsed_query, constraint_count)

    # Process table level constraints:
    constraint_count = get_table_constraints(parsed_query, constraint_count)
    return constraint_count
