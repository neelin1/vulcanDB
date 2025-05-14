import json
import re
from typing import Any, Dict, List, Type
from datetime import datetime
from pydantic import BaseModel, Field

from vulcan.utils.api_helpers import openai_chat_api, openai_chat_api_structured


class ColumnMapping(BaseModel):
    dbColumn: str
    csvColumn: str


class PushSchema(BaseModel):
    mapping: List[ColumnMapping]
    creationOrder: List[str]


def generate_schema(data: dict) -> dict:
    system_prompt = """
### Task ###
Create a relational database schema from the raw data and structure provided by the user.

### Instructions ###
1. Analyze the raw data and its structure provided by the user.
2. Define a relational schema that organizes this data into tables.
3. For each table, specify the columns, their data types, and relationships between tables.
4. Ignore unrelated or redundant columns while generating the schema.
5. Create multiple tables ONLY when it makes sense to do so.
6. If creating multiple tables makes sense. You have raw CSV data with certain columns; you can either:
   - Use a NATURAL primary key from those columns if unique, or
   - If you must create a surrogate column (SERIAL, etc.), you must still keep the original
     CSV column as UNIQUE and NOT NULL, so we can do lookups if needed.
7. If there's a 1:N or N:N relationship, define foreign keys referencing
   the parent's primary or unique columns. 
8. Only split tables when there's a genuine need (like multiple repeated fields).
9. This schema must remain consistent so that once created, we can do 'lookup or insert'
   by the CSV columns if the real PK is synthetic.
10. Refrain from directly generating SQL. Just produce the conceptual schema.
11. If using functions or operators, only use ones that POSTGRESQL/SQLAlchemy supports.
12. Table names should be lower case.

### Input Data ###
1. raw_data: An example of the raw data that will be store in the schema.
2. structure: Information about the datatype for each column

## Desired Output ###
schema: A detailed textual relational schema including table names, column names with data types, and table relationships.
"""
    user_prompt = f"""
### Raw Data Sample ###
{data['raw_data']}


### Raw Data Structure ###
{data['structure']}


Output Schema:
"""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    data["schema"] = openai_chat_api(messages)
    print(">> GENERATED SCHEMA ", data["schema"])
    return data


def generate_push_data_info(
    schema: str, raw_data_structure: str, raw_data_samples: str
) -> PushSchema:
    """
    Produces a JSON object containing:
      - A minimal column mapping (only items that differ between CSV and DB).
      - A creationOrder array specifying the table creation order.

    Returns a PushSchema object with .mapping and .creationOrder
    """

    system_prompt = """
### Task ###
Generate two pieces of information for the client:

1. A minimal "mapping" array describing how CSV columns map to database columns. 
   - Each item is an object { "dbColumn": ..., "csvColumn": ... }.
   - Only include items where the DB column name is different from the CSV column name.
   - Do not map any items related to ids

2. A "creationOrder" array listing tables in the correct order for creation, 
   such that no table depends on a table that appears after it.

### Input Data ###
- schema (the relational schema already generated)
- raw_data_structure (the CSV's columns and data types)
- raw_data_samples (sample data from the CSV)

### Output Format ###
Return valid JSON strictly matching this format:

{
  "mapping": [
    {
      "dbColumn": "str",
      "csvColumn": "str"
    }
    ...
  ],
  "creationOrder": [
    "tableA",
    "tableB",
    ...
  ]
}

No extra text.
"""
    user_prompt = f"""
Schema:
{schema}

Raw Data Structure:
{raw_data_structure}

Raw Data Samples:
{raw_data_samples}

Return:
1. mapping (only changed columns)
2. creationOrder
"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    result = openai_chat_api_structured(
        messages,
        model="gpt-4o",
        temperature=0,
        seed=42,
        response_format=PushSchema,
    )
    return result


def generate_constraints(data: dict) -> dict:
    system_prompt = f"""
### Task ###
Identify constraints in the relational database schema provided by the user.

### Instructions ###
1. Examine the provided raw data and schema.
2. Identify all primary keys (PK) and foreign keys (FK) within the schema.
3. Determine any additional constraints that should be applied to ensure data integrity.
4. Create strict and detailed constraints.
5. Refrain from directly generating SQL Queries.
6. If using functions or operators, only use ones that POSTGRESQL/SQLAlchemy supports.
7. Do not use the ~* operator, it will cause an error.
8. Any column that is referenced as a foreign key must be UNIQUE.
9. If a table uses a surrogate auto-inc PK but also has a CSV column that is unique, 
   ensure that CSV column is UNIQUE NOT NULL.
10. Foreign keys must reference the correct parent PK or unique column.
11. No direct SQL yet, just produce a textual constrained schema description.

### Input Data ###
1. raw_data: An example of the raw data that will be store in the schema.
2. schema: A detailed relational schema including table names, column names with data types, and table relationships.

## Desired Output ###
constrainted schema: A relational schema consisting of all applicable constraints. 
"""
    user_prompt = f"""
### Raw Data Sample ###
{data['raw_data']}


### Schema so far###
{data["schema"]}


Constrained Schema:
"""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    data["constrained_schema"] = openai_chat_api(messages)
    print(">> GENERATED CONSTRAINTS ", data["constrained_schema"])
    return data


def format_sql_queries(queries: str) -> list:
    # Remove the initial ```sql and the final ```
    cleaned_queries = re.sub(
        r"^\s*```sql\s*|\s*```\s*$", "", queries, flags=re.MULTILINE
    )
    # Split the queries by the start of each "CREATE TABLE", using lookahead to keep "CREATE TABLE" with each split
    split_queries = re.split(r"(?=\s*CREATE TABLE)", cleaned_queries.strip())
    # Clean up any leading/trailing whitespace and return the list
    return [query.strip() for query in split_queries if query.strip()]


def generate_sql_queries(data: dict) -> dict:
    system_prompt = f"""
### Task ###
Generate syntactically correct CREATE TABLE queries for the constrained schema provided by the user, specifically for {data["database"]}.

### Instructions ###
1. Using the provided constrained schema, generate CREATE TABLE statements for the {data["database"]} database.
2. Ensure each table includes all specified columns, data types, and constraints.
3. The queries should be syntactically correct to run on a {data["database"]} database.
4. Return only the generated queries.
5. Refrain from using sub-queries in CHECK constraint.
6. Separate each query with double new lines.
7. Ensure all constraints are included in the generated queries.
8. If using functions or operators, only use ones that POSTGRESQL supports. For instance, do not use the ~* operator, it will cause an error.
9. Use quotations around table and column names to allow for different cases.
10. Table names should be lower case.
11. If there's a surrogate PK, also keep the CSV-based column as UNIQUE + NOT NULL for lookups.
12. If foreign keys reference that CSV-based column (or the surrogate PK), do so consistently.
13. Refrain from returning any additional text apart from the queries.

### Key Requirements ###
1. Use NATURAL PRIMARY KEYS from existing columns where possible
2. Foreign keys must reference actual data columns (not surrogate IDs)
3. Add UNIQUE constraints on natural key columns
4. Only use surrogate keys when no suitable natural key combination exists

## Example usage
If 'artist_id' is SERIAL PK, and 'artist_name' is also UNIQUE NOT NULL, referencing might be:
CREATE TABLE "artists" (
    "artist_id" SERIAL PRIMARY KEY,
    "artist_name" VARCHAR UNIQUE NOT NULL
);

Then if a child references "artist_id", do:
FOREIGN KEY("artist_id") REFERENCES "artists"("artist_id")

BUT the child might also reference "artist_name" if the schema said so. Just ensure consistency.
"""

    user_prompt = f"""
### Input Data ###
A relational constrained schema
{data["constrained_schema"]}


## Desired Output ###
CREATE TABLE statements for creating the given constrained schema.


SQL Queries for {data["database"]}:
"""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    queries = openai_chat_api(messages)
    data["queries"] = format_sql_queries(queries)  # type: ignore
    print(">> GENERATED QUERIES ", data["queries"])
    return data
