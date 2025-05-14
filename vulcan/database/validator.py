from typing import List, Dict, Any
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from vulcan.utils.llm_helpers import TableTraitsWithName


def _validate_one_to_n_surrogate_pk_auto_increment(
    engine: Engine, table_traits: List[TableTraitsWithName]
):
    """
    Validates that all 1:n tables have their surrogate primary key configured
    for auto-increment in the database.

    Args:
        engine: SQLAlchemy engine connected to the database.
        table_traits: A list of TableTraitsWithName objects describing the tables.

    Raises:
        ValueError: If a 1:n table's surrogate PK is not auto-incrementing.
    """
    with engine.connect() as connection:
        for trait in table_traits:
            if trait.relation_to_raw == "1:n" and trait.one_to_n:
                table_name = trait.name
                surrogate_pk_col = trait.one_to_n.surrogate_pk_col

                query = text(
                    """
                    SELECT column_name, column_default, is_identity, identity_generation
                    FROM information_schema.columns
                    WHERE table_schema = 'public'  -- Assuming public schema for now
                      AND table_name   = :table_name
                      AND column_name  = :column_name;
                    """
                )
                result = connection.execute(
                    query,
                    {"table_name": table_name, "column_name": surrogate_pk_col},
                )
                column_info = result.fetchone()

                if not column_info:
                    raise ValueError(
                        f"Configuration error for 1:n table '{table_name}': "
                        f"Surrogate PK column '{surrogate_pk_col}' not found in the database schema."
                    )

                col_default = column_info[1]  # column_default
                is_identity = column_info[2]  # is_identity

                is_serial = (
                    col_default is not None and "nextval" in str(col_default).lower()
                )
                is_identity_col = str(is_identity).upper() == "YES"

                if not (is_serial or is_identity_col):
                    raise ValueError(
                        f"Validation Error for 1:n table '{table_name}': "
                        f"Surrogate PK column '{surrogate_pk_col}' is not configured for auto-increment. "
                        f"Details: column_default='{col_default}', is_identity='{is_identity}'"
                    )
                print(f"Validated auto-increment for {table_name}.{surrogate_pk_col}")


def validate_content(
    engine: Engine,
    dataframe: pd.DataFrame,
    table_order: List[str],
    table_traits: List[TableTraitsWithName],
):
    """
    Validates the database content and schema based on table traits and dataframe.

    Args:
        engine: SQLAlchemy engine.
        dataframe: Pandas DataFrame containing the raw data.
        table_order: List of table names in creation order.
        table_traits: List of TableTraitsWithName objects.

    Raises:
        ValueError: If any validation fails.
    """
    print("Starting content validation...")

    # Validate 1:n table surrogate PK auto-increment
    _validate_one_to_n_surrogate_pk_auto_increment(engine, table_traits)

    # Future validation helper calls can be added here

    print("Content validation completed successfully.")
