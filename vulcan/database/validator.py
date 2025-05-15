from typing import List, Dict, Any
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from pydantic import BaseModel, Field
import json

import vulcan.generators.metadata as vgm
from vulcan.utils.llm_helpers import TableTraitsWithName, openai_chat_api_structured


class LLMValidationFeedback(BaseModel):
    feedback: str = Field(
        description="Detailed feedback on violations and suggested changes if any issues are found. If validation passes, this can be a confirmation message."
    )
    continue_processing: bool = Field(
        description="True if validation passes or issues are minor and can be ignored, False if significant violations are found that require correction."
    )


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


def validate_with_llm(
    data_dict: Dict[str, Any],
    dataframe_for_sample: pd.DataFrame,
    max_retries_for_llm_call: int = 1,
) -> LLMValidationFeedback:
    """
    Validates the generated schema, constraints, and traits against a new data sample using an LLM.

    Args:
        data_dict: Dictionary containing keys like 'queries', 'schema', 'constrained_schema', 'table_traits'.
        dataframe_for_sample: The original Pandas DataFrame to draw a new sample from.
        max_retries_for_llm_call: Maximum number of retries for the LLM call if it fails.

    Returns:
        An LLMValidationFeedback object.
    """
    print("Starting LLM-based validation...")

    # Prepare a new data sample
    new_data_sample_str = vgm.get_dataframe_samples(dataframe_for_sample, 10)

    table_traits_list = data_dict.get("table_traits", [])
    if table_traits_list and hasattr(table_traits_list[0], "model_dump_json"):
        table_traits_prompt_string = (
            "[\n"
            + ",\n".join(
                [trait.model_dump_json(indent=2) for trait in table_traits_list]
            )
            + "\n]"
        )
    elif table_traits_list:
        table_traits_prompt_string = json.dumps(table_traits_list, indent=2)
    else:
        table_traits_prompt_string = "[]"

    system_prompt = """
### Task ###
Validate the provided database schema, SQL queries, constraints, and table traits against a sample of raw data. Identify any inconsistencies, violations, or areas for improvement. Specifically, check if:
1. Any columns in the `New Raw Data Sample` would violate the `Generated Constraints` or `Generated SQL Queries` (e.g., a supposedly UNIQUE column having duplicate values in the sample, data type mismatches, NOT NULL violations).
2. The `Table Traits` accurately reflect the relationships (1:1 or 1:n) of tables to the raw data, considering the `New Raw Data Sample`.
3. Column mappings in `Table Traits` are consistent with the `Generated Schema` and the `New Raw Data Sample` column names.
4. Primary and Foreign Key definitions in `Generated SQL Queries` are logical and correctly implemented based on the `Generated Schema` and `Table Traits`.

### Instructions ###
- Provide clear, actionable `feedback`. If issues are found, explain what is wrong and suggest specific changes to the schema, constraints, queries, or traits.
- Set `continue_processing` to `false` if significant issues are found that *must* be corrected. Set it to `true` if all validations pass or if issues are very minor and can be addressed later or ignored.
- Focus on direct violations observable from the sample data against the provided database artifacts.
- Do not suggest stylistic changes unless they directly impact correctness or data integrity.

### Output Format ###
Return a single JSON object strictly matching the following Pydantic model structure:
```json
{
  "feedback": "Detailed feedback...",
  "continue_processing": boolean
}
```
"""

    user_prompt = f"""
### Generated Schema ###
{data_dict.get("schema", "Schema not provided.")}

### Generated Constraints ###
{data_dict.get("constrained_schema", "Constraints not provided.")}

### Generated SQL Queries ###
{data_dict.get("queries", "SQL Queries not provided.")}

### Generated Table Traits ###
{table_traits_prompt_string}

### New Raw Data Sample (for validation) ###
{new_data_sample_str}

Based on the above, please provide your validation feedback:
"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    for attempt in range(max_retries_for_llm_call):
        try:
            validation_response = openai_chat_api_structured(
                messages,
                model="gpt-4.1",
                temperature=0,
                seed=43,
                response_format=LLMValidationFeedback,
            )
            print(
                f">> LLM Validation Feedback: {validation_response.model_dump_json(indent=2)}"
            )
            return validation_response
        except Exception as e:
            print(
                f">> ERROR during LLM validation (attempt {attempt + 1}/{max_retries_for_llm_call}): {e}"
            )
            if attempt + 1 == max_retries_for_llm_call:
                # Last attempt failed, return a default "error" feedback
                return LLMValidationFeedback(
                    feedback=f"LLM-based validation failed after {max_retries_for_llm_call} attempts due to: {e}. Cannot proceed with this validation step.",
                    continue_processing=False,
                )
    # Should not be reached if loop completes, but as a fallback:
    return LLMValidationFeedback(
        feedback="LLM-based validation could not be completed due to an unexpected issue.",
        continue_processing=False,
    )


def validate_content(
    engine: Engine,
    dataframe: pd.DataFrame,
    table_order: List[str],
    table_traits: List[TableTraitsWithName],
    single_table: bool = False,
):
    """
    Validates the database content and schema based on table traits and dataframe.

    Args:
        engine: SQLAlchemy engine.
        dataframe: Pandas DataFrame containing the raw data.
        table_order: List of table names in creation order.
        table_traits: List of TableTraitsWithName objects.
        single_table: Boolean flag, if True, enforces a single table in the schema.

    Raises:
        ValueError: If any validation fails.
    """
    print("Starting content validation...")

    # Validate single table constraint if flag is true
    if single_table and len(table_order) > 1:
        raise ValueError(
            f"Single table flag is true, but schema contains {len(table_order)} tables: {', '.join(table_order)}."
        )

    # Validate 1:n table surrogate PK auto-increment
    _validate_one_to_n_surrogate_pk_auto_increment(engine, table_traits)

    # Future validation helper calls can be added here

    print("Content validation completed successfully.")
