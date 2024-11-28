from typing import Optional, Tuple

from pandas import DataFrame
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from vulcan.database.load import push_data_in_db


def initialize_default_database(db_file: str = "default.db") -> Engine:
    """
    Initializes a SQLite database engine with a default or specified database file.

    Parameters:
    - db_file: Name of the SQLite database file. Defaults to 'default.db'.

    Returns:
    - SQLAlchemy Engine instance for the SQLite database.
    """
    print("Initializing SQLITE Database")
    db_uri = f"sqlite:///output/{db_file}"
    return create_engine(db_uri, echo=True, future=True)


def initialize_postgres_database(
    db_uri: str, connect_args: Optional[dict] = None, **engine_kwargs
) -> Engine:
    """
    Initializes a PostgreSQL database engine.

    Parameters:
    - db_uri: PostgreSQL database URI for connection.
    - connect_args: Optional dictionary of connection arguments to be passed to the database.
    - engine_kwargs: Additional keyword arguments to be passed to create_engine.

    Returns:
    - SQLAlchemy Engine instance for the PostgreSQL database.
    """
    print("Initializing POSTGRESQL Database")
    if not db_uri.startswith("postgresql://"):
        raise ValueError("Invalid URI: db_uri must start with 'postgresql://'")
    if connect_args is None:
        connect_args = {}
    return create_engine(db_uri, echo=True, connect_args=connect_args, **engine_kwargs)


def initialize_database(
    db_uri: str,
    db_type: str = "postgres",
    connect_args: Optional[dict] = None,
    **engine_kwargs,
) -> Engine:
    """
    Initializes a database engine.

    Parameters:
    - db_uri: Database URI for connection.
    - connect_args: Optional dictionary of connection arguments to be passed to the database.
    - engine_kwargs: Additional keyword arguments to be passed to create_engine.

    Returns:
    - SQLAlchemy Engine instance.
    """
    if db_type == "postgres" and db_uri:
        return initialize_postgres_database(db_uri, connect_args, **engine_kwargs)
    elif db_type == "sqlite":
        return initialize_default_database()
    else:
        raise ValueError(f"Unsupported db_type: {db_type}")


def execute_queries(
    engine: Engine, table_order: list[str], tables: dict
) -> Tuple[bool, Optional[str]]:
    """
    Executes a list of SQL queries using the given engine.

    Parameters:
    - engine: SQLAlchemy Engine instance.
    - queries: List of SQL query strings to be executed.

    Returns:
    - Tuple of success flag and error message (if any).
    """
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            for table_name in table_order:
                query = tables[table_name]["query"]
                conn.execute(text(query))
            transaction.commit()
            return True, None
        except SQLAlchemyError as e:
            transaction.rollback()
            return False, e


def reset_database(engine: Engine):
    """
    Resets the database by dropping all tables. Use with caution.
    """
    meta = MetaData()
    meta.reflect(bind=engine)
    meta.drop_all(bind=engine)


def populate_database(
    db_uri: str,
    table_order: list[str],
    tables: dict,
    dataframe: DataFrame,
    alias_mapping: dict,
    connect_args: Optional[dict] = None,
    **engine_kwargs,
):
    engine = initialize_database(
        db_uri=db_uri, db_type="postgres", connect_args=connect_args, **engine_kwargs
    )
    execute_queries(engine, table_order, tables)
    push_data_in_db(engine, dataframe, table_order, alias_mapping)
    engine.dispose()
