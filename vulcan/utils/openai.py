import json
import os
import re
from typing import Any, Dict, List, Type
from datetime import datetime

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field


load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


def openai_chat_api(messages, *, model="gpt-4o", temperature=0, seed=42):
    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        messages=messages, model=model, temperature=temperature, seed=seed
    )
    return response.choices[0].message.content


class ColumnMapping(BaseModel):
    dbColumn: str
    csvColumn: str


class PushSchema(BaseModel):
    mapping: List[ColumnMapping]
    creationOrder: List[str]


def openai_chat_api_structured(
    messages, *, model="gpt-4o", temperature=0, seed=42, response_format=None
):
    """
    Similar to openai_chat_api, but enforces a structured output
    using the Beta OpenAI API features for structured JSON output.
    """
    client = OpenAI(api_key=OPENAI_API_KEY)
    # enforces schema adherence with response_format
    completion = client.beta.chat.completions.parse(
        messages=messages,
        model=model,
        temperature=temperature,
        seed=seed,
        response_format=response_format,  # type: ignore
    )

    structured_response = completion.choices[0].message
    # Catch refusals
    if structured_response.refusal:
        raise ValueError(
            "OpenAI refused to complete input: " + structured_response.refusal
        )
    elif structured_response.parsed:
        return structured_response.parsed
    else:
        raise ValueError("No structured output or refusal was returned.")


def generate_schema(data: dict) -> dict:
    system_prompt = """
### Task ###
Create a relational database schema from the raw data and structure provided by the user.

### Instructions ###
1. Analyze the raw data and its structure provided by the user.
2. Define a relational schema that organizes this data into tables.
3. For each table, specify the columns, their data types, and relationships between tables.
4. Ignore unrelated or redundant columns while generating the schema.
5. Create multiple tables ONLY when it is required
6. Use the auto increment clause for primary key if required.
7. Refrain from directly generating SQL Queries.
8. If using functions or operators, only use ones that POSTGRESQL supports.
9. Table names should be lower case.

### Input Data ###
1. raw_data: An example of the raw data that will be store in the schema.
2. structure: Information about the datatype for each column

## Desired Output ###
schema: A detailed relational schema including table names, column names with data types, and table relationships.
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
6. If using functions or operators, only use ones that POSTGRESQL supports.
8. DO NOT USE the UNIQUE constraint. It causes too many issues because the sample is not always representative of the full data. 
9. Do not use the ~* operator, it will cause an error.

### Input Data ###
1. raw_data: An example of the raw data that will be store in the schema.
2. schema: A detailed relational schema including table names, column names with data types, and table relationships.

## Desired Output ###
constrainted schema: A relational schema consisting of all applicable constraints. 
"""
    user_prompt = f"""
### Raw Data Sample ###
{data['raw_data']}


### Schema ###
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
5. Refrain from returning any additional text apart from the queries.
6. Separate each query with double new lines.
7. Ensure all constraints are included in the generated queries.
8. If using functions or operators, only use ones that POSTGRESQL/MO_SQL_PARSING/SQLITE support.
9. Use quotations around table and column names to allow for different cases.
10. Table names should be lower case.
9. Do not use the ~* operator, it will cause an error.


### Example ###
Suppose the schema provided is:
employees
  - Columns:
    - id INT PRIMARY KEY
    - name VARCHAR(100) NOT NULL
    - department_id INT REFERENCES Departments(id)
  - Constraints:
    - id is the primary key.
    - name must not be null.
    - age must be greater than 18 (Check Constraint).

Based on the above schema the output should be:
CREATE TABLE "employees" (
    "id" INT PRIMARY KEY,
    "name" VARCHAR(100) NOT NULL,
    "age" INT CHECK (age > 18),
    "salary" DECIMAL(10, 2)
);
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
