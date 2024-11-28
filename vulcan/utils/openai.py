import json
import os
import re
from typing import Any, Dict, Type
from datetime import datetime

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field


load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

unsupported = "ON DELETE RESTRICT, ON DELETE CASCADE, ~, ~*"


def openai_chat_api(messages, *, model="gpt-4o", temperature=0, seed=42):
    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        messages=messages, model=model, temperature=temperature, seed=seed
    )
    return response.choices[0].message.content


def openai_chat_api_structured(
    messages, response_format: Type[BaseModel], model="gpt-4o", temperature=0, seed=42
):
    client = OpenAI(api_key=OPENAI_API_KEY)
    completion = client.beta.chat.completions.parse(
        messages=messages,
        model=model,
        temperature=temperature,
        seed=seed,
        response_format=response_format,
    )
    structured_response = completion.choices[0].message
    if structured_response.parsed:
        return structured_response.parsed
    elif structured_response.refusal:
        raise ValueError("OpenAI refused to complete input")


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
8. If using functions or operators, only use ones that POSTGRESQL/MO_SQL_PARSING/SQLITE support. Do not use functions/ops like {unsupported}.
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


def generate_alias_mapping(data: dict) -> dict:
    system_prompt = """
### Task ###
Generate a mapping between the column names used in the schema and the original column names from the raw data.

### Instructions ###
1. Analyze the provided schema and raw data structure.
2. Identify any columns in the schema that have been renamed from the original data.
3. Create a mapping where keys are the schema column names and values are the original column names found inn the raw data structure.
4. Output the mapping in JSON format.

### Input Data ###
1. schema: The relational schema with the possibly renamed columns.
2. raw_data_structure: The original column names and data types from the raw data.

### Desired Output ###
A JSON object representing the alias mapping with no added text above or below the curly braces other than ```json ... ```. Map string to string, do not include any string to lists. Do not repeated schema_column_names multiple times:
```json
{
  "schema_column_name1": "original_column_name1",
  "schema_column_name2": "original_column_name2",
  ...
}
```
"""
    user_prompt = f"""
### Schema ###
{data['schema']}

### Raw Data Structure ###
{data['structure']}

### Raw Data Samples ###
{data['raw_data']}

Alias Mapping:
"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    mapping_output = openai_chat_api(messages)
    print(">> GENERATED ALIAS MAPPING ", mapping_output)

    data["alias_mapping"] = {}
    try:
        if mapping_output:
            cleaned_output = re.search(r"{.*}", mapping_output, re.DOTALL)
            if cleaned_output:
                alias_mapping = json.loads(cleaned_output.group(0))
                data["alias_mapping"] = alias_mapping
            else:
                raise ValueError("Failed to extract JSON from the output")
        else:
            raise ValueError("No mapping output generated")
    except (json.JSONDecodeError, ValueError) as e:
        print(f"ERROR: Failed to parse alias mapping output: {e}")
        print("Error Causing Mapping Generation:", mapping_output)

    print(">> OUTPUTTED ALIAS MAPPING ", data["alias_mapping"])
    return data


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
6. If using functions or operators, only use ones that POSTGRESQL/MO_SQL_PARSING/SQLITE support. Do not use functions/ops like {unsupported}.
7. Table names should be lower case.
8. DO NOT USE the UNIQUE constraint. It causes too many issues because the sample is not always representative of the full data. 

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
8. If using functions or operators, only use ones that POSTGRESQL/MO_SQL_PARSING/SQLITE support. Do not use functions/ops like {unsupported}.
9. Table names should be lower case.

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
CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    age INT CHECK (age > 18),
    salary DECIMAL(10, 2)
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
    data["queries"] = format_sql_queries(queries)
    print(">> GENERATED QUERIES ", data["queries"])
    return data
