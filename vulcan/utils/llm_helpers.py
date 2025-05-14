import json
import re
from typing import Any, Dict, List, Type, Optional, Literal
from datetime import datetime
from pydantic import BaseModel, Field

from vulcan.utils.api_helpers import openai_chat_api, openai_chat_api_structured


def generate_schema(data: dict) -> dict:
    system_prompt = """
### Task ###
Create a relational database schema from the raw data and structure provided by the user.

### Instructions ###
1. Analyze the raw data and its structure provided by the user.
2. Define a relational schema that organizes this data into tables.
3. For each table, specify the columns, their data types, and relationships between tables.
4. Ignore unrelated or redundant columns while generating the schema.
5. Refrain from directly generating SQL. Just produce the conceptual schema.
6. If using functions or operators, only use ones that POSTGRESQL/SQLAlchemy supports.
7. Table names must be lower case.
8. Create multiple tables ONLY when it makes sense to do so.
9. Focus on the following allowed table structures:
    a. Tables with a one-to-one (1:1) relationship to rows in the raw data. These can be:
        i. Dependency-free.
        ii. Dependent on another 1:1 table.
        iii. Dependent on a one-to-many (1:n) table.
    b. Tables with a one-to-many (1:n) relationship to the raw data (i.e., they consolidate unique values from columns in the raw data). These can be:
        i. Dependency-free.
        ii. Dependent on another 1:n table.
10. Do NOT create the following types of tables or relationships:
    a. Many-to-many (N:N) relationships.
    b. Junction tables.
    c. Self-referential relationships (e.g., an employee table where a row references another row in the same table as a manager).
11. If creating a 1:N table, descibe its primary key and surrogate pk (ex: employer_name and employer_name_id). The surrogate pk column name should be the pk column name with _id suffix.
12. Generally refrain from renaming columns from the raw data, but if you do, explain why.

### Input Data ###
1. raw_data: An example of the raw data that will be store in the schema.
2. structure: Information about the datatype for each column

## Desired Output ###
schema: A detailed textual relational schema including table names, column names with data types, and table relationships.

Example Output Schema:
#### High Level Explanation ####
paragraph explaining the overall structure of how we are going to model the data

#### Table: people #### 
## Traits:
- 1:1 with raw data
- depends on: 
    - employers (which is 1:N with raw data)

## Columns:
- id: INTEGER PRIMARY KEY (1:1 row id)
- name: VARCHAR (direct mapping from raw$name)
- employer_id: INTEGER FOREIGN KEY REFERENCES employers(employer_id) (foreign key to 1:N table employers)
- work_email: VARCHAR (renamed direct mapping from raw$work_email_address)
...
## Explanation:
paragraph explaining the table and its relationships with other tables and the raw data

#### Table: employers #### 
## Traits:
- 1:N with raw data
- dependency free

## Columns:
- employer_id: INTEGER PRIMARY KEY (surrogate key for natural key employer)
- employer: VARCHAR NOT NULL UNIQUE (natural key, direct mapping from raw$employer)
...
## Explanation:
...
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


class TableList(BaseModel):
    table_names: List[str] = Field(
        description="A list of table names extracted from the schema."
    )


def generate_table_list(data: dict) -> dict:
    """
    Generates a list of table names from the schema.
    """
    system_prompt = """
### Task ###
Extract all table names from the provided database schema.

### Instructions ###
1. Analyze the schema provided by the user.
2. Identify each table defined within the schema.
3. Return a list containing only the names of these tables.
4. Ensure the output is a JSON object with a single key "table_names" whose value is a list of strings.
5. All table names should be lower case.

### Input Data ###
schema: A detailed textual relational schema.

## Desired Output ###
A JSON object with a single key "table_names" containing a list of table names.
For example:
{
  "table_names": ["table_a", "table_b", "table_c"]
}
"""
    user_prompt = f"""
### Schema ###
{data['schema']}

Output a JSON object containing the list of table names:
"""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    table_list_response = openai_chat_api_structured(
        messages,
        model="gpt-4.1",
        temperature=0,
        seed=42,
        response_format=TableList,
    )
    data["table_list"] = table_list_response.table_names
    print(">> GENERATED TABLE LIST ", data["table_list"])
    return data


class ColumnMappingDetail(BaseModel):
    raw_csv_col: str = Field(description="Column name in the raw CSV file.")
    table_col: str = Field(
        description="Corresponding column name in the database table."
    )


class OneToNTraitDetail(BaseModel):
    surrogate_pk_col: str = Field(
        description="Name of the surrogate primary key column (e.g., 'artist_id')."
    )
    natural_key_col: str = Field(
        description="Column name that forms the natural key (e.g., 'artist_name')."
    )


class DependencyDetail(BaseModel):
    parent_table_name: str = Field(
        description="Name of the parent table this table depends on."
    )
    local_fk_col: str = Field(
        description="Name of the foreign key column in the current table that references the parent table."
    )


class SingleTableTraits(BaseModel):
    relation_to_raw: Literal["1:1", "1:n"] = Field(
        description="Relationship of the table to the raw data rows ('1:1' or '1:n')."
    )
    mapping: List[ColumnMappingDetail] = Field(
        default_factory=list,
        description="List of column mappings where CSV and DB column names differ. Empty if all names are identical or no direct CSV mapping.",
    )
    one_to_n: Optional[OneToNTraitDetail] = Field(
        default=None,
        description="Details for 1:n tables, including surrogate PK and natural key. Present if relation_to_raw is '1:n', otherwise null.",
    )
    dependencies: List[DependencyDetail] = Field(
        default_factory=list,
        description="List of tables this table depends on, with foreign key details. Empty if no dependencies.",
    )


class TableTraitsWithName(SingleTableTraits):
    name: str = Field(description="Name of the table.")


def generate_table_traits(data: dict) -> dict:
    """
    Generates detailed traits for each table in the schema.
    For each table, it identifies its relation to raw data, column mappings (only for differing names),
    1:n specific details (surrogate PK, natural keys), and dependencies.
    """
    schema_text = data.get("schema", "")
    raw_data_structure = data.get(
        "structure", ""
    )  # Assuming 'structure' holds raw data structure
    table_list = data.get("table_list", [])

    if not schema_text or not raw_data_structure or not table_list:
        print(
            ">> SKIPPING TRAIT GENERATION: Missing schema, raw_data_structure, or table_list in data."
        )
        data["table_traits"] = []
        return data

    all_table_traits: List[TableTraitsWithName] = []

    system_prompt = """
### Task ###
For a single database table, extract its structural traits based on the provided overall schema, raw data structure, and table name. The traits include its relationship to raw data, column mappings (only if names differ from CSV), details for 1:n relationships (surrogate PK, natural keys), and dependencies on other tables.

### Input Data ###
1.  `schema`: The complete relational schema description.
2.  `raw_data_structure`: The structure of the source CSV/raw data (column names and types).
3.  `table_name`: The specific table for which to extract traits.

### Instructions for Trait Extraction ###
1.  **`relation_to_raw`**: Determine if the table has a "1:1" or "1:n" relationship with the raw data rows. This is typically found in the "Traits" section for the table in the schema.
    - Example "1:1": "Traits: - 1:1 correspondence with raw data rows"
    - Example "1:n": "Traits: - 1:N correspondence with raw data rows"

2.  **`mapping`**: List column mappings ONLY where the database `table_col` name is DIFFERENT from the `raw_csv_col` name.
    - Check the "Columns" section of the table in the schema. Look for mentions like "(direct mapping from raw$csv_column_name)" or "(renamed direct mapping from raw$csv_column_name)".
    - If a schema column `db_col` comes from `raw$csv_col` and `db_col` is different from `csv_col`, include `{"raw_csv_col": "csv_col", "table_col": "db_col"}`.
    - If all mapped columns from raw data retain their original names, or if a column is not directly from raw data (e.g., a primary key), this list should be empty.

3.  **`one_to_n`**: If `relation_to_raw` is "1:n", provide this object. Otherwise, this field MUST be null or omitted.
    -   **`surrogate_pk_col`**: Identify the surrogate primary key column for the 1:n table. This is often an auto-incrementing ID column (e.g., `employer_id`). The schema might state "(surrogate key for natural key...)".
    -   **`natural_key_col`**: Identify the column name that forms the natural key for the 1:n table. This is a column from the raw data that uniquely identifies records in the conceptual 1:n entity (e.g., `employer_name`). Schema might state "(natural key, direct mapping from raw$...)".

4.  **`dependencies`**: List tables that the current table depends on via foreign keys.
    - This information is usually in the "Traits" section (e.g., "depends on: parent_table_name") or in column definitions (e.g., "FOREIGN KEY REFERENCES parent_table_name(parent_pk_col)").
    -   **`parent_table_name`**: The name of the table that the current table's foreign key references.
    -   **`local_fk_col`**: The name of the column in the *current* table that acts as the foreign key.
    - If no dependencies, this list should be empty.

### Allowed Table Structures (from schema generation rules) ###
- Tables with a one-to-one (1:1) relationship to rows in the raw data. These can be:
    - Dependency-free.
    - Dependent on another 1:1 table.
    - Dependent on a one-to-many (1:n) table.
- Tables with a one-to-many (1:n) relationship to the raw data. These can be:
    - Dependency-free.
    - Dependent on another 1:n table.
- 1:N tables should have a surrogate primary key (e.g., `column_name_id`) and a natural key (e.g., `column_name`).

### Output Format ###
Return a single JSON object strictly matching the following Pydantic model structure (do not include the model definition in the output, just the JSON data itself):
```python
class SingleTableTraits(BaseModel):
    relation_to_raw: Literal["1:1", "1:n"]
    mapping: List[ColumnMappingDetail] # where ColumnMappingDetail is {"raw_csv_col": str, "table_col": str}
    one_to_n: Optional[OneToNTraitDetail] # where OneToNTraitDetail is {"surrogate_pk_col": str, "natural_key_col": str}
    dependencies: List[DependencyDetail] # where DependencyDetail is {"parent_table_name": str, "local_fk_col": str}
```
Ensure `mapping` and `dependencies` are provided as empty lists if no such items exist. `one_to_n` MUST be provided if `relation_to_raw` is '1:n' and MUST be `null` (or omitted) if `relation_to_raw` is '1:1'.
"""

    for table_name in table_list:
        user_prompt = f"""
### Schema ###
{schema_text}

### Raw Data Structure ###
{raw_data_structure}

### Target Table Name ###
{table_name}

Extract traits for the table "{table_name}":
"""
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        try:
            table_traits_response = openai_chat_api_structured(
                messages,
                model="gpt-4.1",
                temperature=0,
                seed=42,
                response_format=SingleTableTraits,
            )
            # Combine with table_name and append
            full_traits = TableTraitsWithName(
                name=table_name, **table_traits_response.model_dump()
            )
            all_table_traits.append(full_traits)
            print(f">> GENERATED TRAITS FOR TABLE: {table_name}")
        except Exception as e:
            print(f">> ERROR generating traits for table {table_name}: {e}")
            # Optionally, append a placeholder or skip
            all_table_traits.append(
                TableTraitsWithName(
                    name=table_name, relation_to_raw="1:1", mapping=[], dependencies=[]
                )
            )  # Basic default

    data["table_traits"] = all_table_traits
    print(
        ">> ALL TABLE TRAITS GENERATED: ",
        [trait.model_dump_json(indent=2) for trait in all_table_traits],
    )
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

### Table Traits ###
{data["table_traits"]}


Constrained Schema:
"""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    data["constrained_schema"] = openai_chat_api(messages)
    print(">> GENERATED CONSTRAINTS ", data["constrained_schema"])
    return data


def generate_sql_queries(data: dict) -> dict:
    system_prompt = f"""
### Task ###
Generate syntactically correct CREATE TABLE queries for the constrained schema provided by the user, specifically for {data["database"]}.

### Instructions ###
1. Using the provided constrained schema, generate CREATE TABLE statements for the {data["database"]} database.
2. Ensure each table includes all specified columns, data types, and constraints mentioned in the `Constrained Schema`.
3. The queries should be syntactically correct to run on a {data["database"]} database.
4. Return only the generated queries.
5. Refrain from using sub-queries in CHECK constraint.
6. Separate each query with double new lines.
7. Ensure all constraints from the `Constrained Schema` are included in the generated queries.
8. If using functions or operators, only use ones that POSTGRESQL supports. For instance, do not use the ~* operator, it will cause an error.
9. Use quotations around table and column names to allow for different cases (e.g., "TableName", "ColumnName").
10. Table names should be lower case and perfectly match the schema generation (e.g., "tablename").
11. If there's a surrogate PK (e.g., SERIAL), also ensure that any associated natural key / CSV-based unique column(s) from the `Constrained Schema` are marked `UNIQUE NOT NULL`.
12. If foreign keys reference a natural key column (that is also `UNIQUE NOT NULL`) or a surrogate PK, do so consistently based on the `Constrained Schema`.
13. Refrain from returning any additional text apart from the queries.
14. (If `Table Traits` are provided and contain relevant entries): For tables identified as '1:n' in `Table Traits` (via `relation_to_raw: \"1:n\"` and the presence of `one_to_n` details):
    a. Ensure all columns listed in `one_to_n.natural_key_col` are defined as `UNIQUE NOT NULL` in the `CREATE TABLE` statement.
    b. The column specified in `one_to_n.surrogate_pk_col` should be the `PRIMARY KEY` (typically `SERIAL PRIMARY KEY` or equivalent for auto-incrementing behavior).
    c. This information from `Table Traits` complements and helps clarify the `Constrained Schema`. If there's a conflict, prioritize the structured `Table Traits` for these specific details (surrogate PK, natural keys for 1:n tables).

### Key Requirements (Derived from `Constrained Schema`) ###
1. Use natural primary keys from existing columns if the `Constrained Schema` indicates them as PKs and no surrogate key is specified.
2. Ensure all columns designated as `UNIQUE` or part of a natural key in the `Constrained Schema` have `UNIQUE` constraints (and `NOT NULL` if appropriate, especially for natural keys).
"""

    table_traits_list = data.get("table_traits", [])
    # Ensure table_traits_list is a list of objects that have model_dump_json if it's not empty
    if table_traits_list and hasattr(table_traits_list[0], "model_dump_json"):
        table_traits_json_list = [
            trait.model_dump_json(indent=2) for trait in table_traits_list
        ]
        table_traits_prompt_string = (
            "[\\n" + ",\\n".join(table_traits_json_list) + "\\n]"
        )
    elif table_traits_list:  # If it's already dicts/simple structures
        table_traits_prompt_string = json.dumps(table_traits_list, indent=2)
    else:
        table_traits_prompt_string = "[]"

    user_prompt = f"""
### Constrained Schema ###
{data["constrained_schema"]}

### Table Traits (for additional context, especially for 1:n tables and their keys) ###
{table_traits_prompt_string}

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


def format_sql_queries(queries: str) -> list:
    # Remove the initial ```sql and the final ```
    cleaned_queries = re.sub(
        r"^\s*```sql\s*|\s*```\s*$", "", queries, flags=re.MULTILINE
    )
    # Split the queries by the start of each "CREATE TABLE", using lookahead to keep "CREATE TABLE" with each split
    split_queries = re.split(r"(?=\s*CREATE TABLE)", cleaned_queries.strip())
    # Clean up any leading/trailing whitespace and return the list
    return [query.strip() for query in split_queries if query.strip()]
