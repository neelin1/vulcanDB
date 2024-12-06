from pglast import parse_sql
from pglast.enums import ConstrType
from pglast.ast import CreateStmt, ColumnDef, Constraint, RawStmt
from typing import Dict, List

import vulcan.parsers.query as vpq


def get_column_constraints(
    columns: List[ColumnDef], constraint_count: Dict[str, int]
) -> Dict[str, int]:
    """Process column-level constraints from ColumnDef objects."""
    for column in columns:
        for constraint in column.constraints or []:
            if isinstance(constraint, Constraint):
                if constraint.contype == ConstrType.CONSTR_PRIMARY:
                    constraint_count["primary_key"] += 1
                elif constraint.contype == ConstrType.CONSTR_NOTNULL:
                    constraint_count["not_null"] += 1
                elif constraint.contype == ConstrType.CONSTR_UNIQUE:
                    constraint_count["unique"] += 1
                elif constraint.contype == ConstrType.CONSTR_DEFAULT:
                    constraint_count["default"] += 1
                elif constraint.contype == ConstrType.CONSTR_CHECK:
                    constraint_count["check"] += 1
    return constraint_count


def get_table_constraints(
    constraints: List[Constraint], constraint_count: Dict[str, int]
) -> Dict[str, int]:
    """Process table-level constraints from Constraint objects."""
    for constraint in constraints:
        if constraint.contype == ConstrType.CONSTR_FOREIGN:
            constraint_count["foreign_key"] += 1
        elif constraint.contype == ConstrType.CONSTR_CHECK:
            constraint_count["check"] += 1
        elif constraint.contype == ConstrType.CONSTR_UNIQUE:
            constraint_count["unique"] += 1
        elif constraint.contype == ConstrType.CONSTR_PRIMARY:
            constraint_count["primary_key"] += 1
    return constraint_count


def count_constraints(sql_query):
    """Count the different types of constraints in a CREATE TABLE statement."""
    # Initialize the counter for constraints
    constraint_count = {
        "primary_key": 0,
        "foreign_key": 0,
        "unique": 0,
        "check": 0,
        "not_null": 0,
        "default": 0,
    }

    # Parse the SQL query into a JSON structure
    parsed_statements = parse_sql(sql_query)

    columns = vpq.extract_columns_from_parsed_query(parsed_statements)
    constraints = vpq.extract_table_constraints_from_parsed_query(parsed_statements)
    # Process column level constraints:
    constraint_count = get_column_constraints(columns, constraint_count)
    # Process table level constraints:
    constraint_count = get_table_constraints(constraints, constraint_count)
    return constraint_count
