from dotenv import load_dotenv
import pandas as pd  # Added for type hinting if not already present elsewhere
from typing import Optional

from vulcan.generators.query import generate_sql_queries
from vulcan.parsers.graph import create_query_dependent_graph
from vulcan.parsers.dependency import determine_table_creation_order
from vulcan.database.core import initialize_database, execute_queries
from vulcan.database.validator import (
    validate_content,
    validate_with_llm,
    LLMValidationFeedback,
)
from vulcan.database.load import push_data_in_db


load_dotenv()


def run_pipeline(dataframe: pd.DataFrame, db_uri: str, single_table: bool):
    max_attempts = 2
    feedback_for_rerun: Optional[str] = None
    data_dict = None
    final_validation_passed = False

    for attempt in range(max_attempts):
        print(f"Starting generation pipeline attempt {attempt + 1}/{max_attempts}")
        if feedback_for_rerun:
            print(f"Applying feedback from previous attempt: {feedback_for_rerun}")

        # Generate Schema, Constraints, and Queries
        data_dict = generate_sql_queries(
            dataframe, single_table, feedback_from_validation=feedback_for_rerun
        )

        queries = data_dict.get("queries")
        if not queries:
            # This case should ideally be handled by generate_sql_queries or earlier steps
            # If queries are empty, it might indicate a fundamental failure in generation.
            # For now, if it happens on the first run, we allow a retry with general feedback.
            # If it persists, it will fail the pipeline.
            print("Error: No SQL queries were generated.")
            if attempt < max_attempts - 1:
                feedback_for_rerun = "No SQL queries were generated. Please review the schema and constraints to ensure valid queries can be created."
                # Clear potentially problematic parts of data_dict for the retry, or let them be overwritten
                data_dict["queries"] = []  # Ensure it's an empty list, not None
                data_dict["constrained_schema"] = (
                    data_dict.get("constrained_schema", "")
                    + "\nFeedback: No queries generated, check for issues."
                )
                continue  # Try again with this feedback
            else:
                raise ValueError(
                    "Failed to generate SQL queries after multiple attempts."
                )

        if "table_traits" not in data_dict or "table_list" not in data_dict:
            # This indicates a failure in earlier generation steps (e.g., table list or traits generation)
            error_message = (
                "table_traits or table_list is missing in data_dict after generation."
            )
            print(f"Error: {error_message}")
            if attempt < max_attempts - 1:
                feedback_for_rerun = (
                    error_message + " Please ensure these are generated correctly."
                )
                # Potentially clear/reset parts of data_dict if needed for retry logic
                data_dict["table_traits"] = data_dict.get("table_traits", [])
                data_dict["table_list"] = data_dict.get("table_list", [])
                continue  # Try again
            else:
                raise ValueError(
                    f"{error_message} Failed after {max_attempts} attempts."
                )

        table_traits = data_dict["table_traits"]
        table_list = data_dict["table_list"]

        current_run_feedback_items = []
        # Initialize llm_can_continue to True. It will only be set to False
        # if LLM validation runs (on attempt 0) and fails.
        llm_can_continue = True
        llm_feedback_for_this_run = None  # Store LLM feedback from the first attempt

        # 1. LLM-based validation - only on the first attempt
        if attempt == 0:
            print("Running LLM-based validation (first attempt only)...")
            llm_validation_result = validate_with_llm(data_dict, dataframe)
            llm_feedback_for_this_run = (
                llm_validation_result.feedback
            )  # Store for potential use in combined feedback
            if not llm_validation_result.continue_processing:
                llm_can_continue = False
                # Add LLM feedback to current_run_feedback_items only if it's a failure
                current_run_feedback_items.append(
                    f"LLM Validator: {llm_validation_result.feedback}"
                )
                print(f"LLM Validation failed: {llm_validation_result.feedback}")
            else:
                print(f"LLM Validation reported: {llm_validation_result.feedback}")
        else:
            print("Skipping LLM-based validation on subsequent attempts.")

        # Initialize database and create schema only if LLM validation allows or if we are to proceed anyway for deterministic checks
        # For now, let's proceed to deterministic validation even if LLM has concerns,
        # as deterministic errors might be more critical for the retry feedback.

        # Initialize the database engine (do this before execute_queries)
        engine = initialize_database(db_uri=db_uri)

        # Create the dependent graph (still useful for getting the 'tables' dictionary structure)
        dependent_graph, tables_dict_from_graph = create_query_dependent_graph(queries)
        print(">> Dependent Graph:", dependent_graph)
        print(">> Tables Dict from Graph:", tables_dict_from_graph)

        # Determine table creation order
        table_order = determine_table_creation_order(table_traits, table_list)
        print(">> Determined Table Order:", table_order)

        # Create tables by executing the CREATE statements in the correct order
        # This part needs the engine, so it comes after initialize_database
        success_create, error_create = execute_queries(
            engine, table_order, tables_dict_from_graph
        )
        if not success_create:
            error_message = f"Table creation error: {error_create}"
            print(error_message)
            current_run_feedback_items.append(f"Schema Execution: {error_message}")

            if attempt < max_attempts - 1:
                # Consolidate feedback for rerun
                # If LLM validation ran (attempt == 0) and had critical feedback not already included, prepend it.
                # This was simplified: current_run_feedback_items already includes LLM feedback if it failed.
                feedback_for_rerun = "\n".join(current_run_feedback_items)
                continue  # Go to next attempt
            else:
                raise Exception(
                    f"Table creation failed after {max_attempts} attempts: {error_create}"
                )
        else:
            print(f"Tables created successfully for attempt {attempt + 1}!")

        # 2. Deterministic validation (validate_content)
        deterministic_validation_passed = False
        try:
            print("Running deterministic validation (validate_content)...")
            validate_content(engine, dataframe, table_order, table_traits, single_table)
            print("Deterministic schema validation (validate_content) passed!")
            deterministic_validation_passed = True
        except ValueError as e_deterministic:
            print(
                f"Deterministic schema validation (validate_content) failed: {e_deterministic}"
            )
            current_run_feedback_items.append(
                f"Deterministic Validator: {str(e_deterministic)}"
            )

        # Check if the overall attempt was successful
        if llm_can_continue and deterministic_validation_passed:
            final_validation_passed = True
            print(f"Attempt {attempt + 1} successful. Pipeline proceeding.")
            break  # Exit loop, successful run
        else:
            # Attempt failed
            if attempt < max_attempts - 1:
                feedback_for_rerun = "\n".join(current_run_feedback_items)
                print(
                    f"Attempt {attempt + 1} failed. Consolidating feedback for next attempt: {feedback_for_rerun}"
                )
                # `continue` is implicit here, loop will proceed to next iteration
            else:
                # Max attempts reached, and this attempt also failed
                print(f"All {max_attempts} attempts failed.")
                error_summary = (
                    "\n".join(current_run_feedback_items)
                    if current_run_feedback_items
                    else "Unknown validation errors after multiple attempts."
                )
                raise Exception(
                    f"Pipeline failed after {max_attempts} attempts. Last errors: {error_summary}"
                )

    # If loop finished without final_validation_passed (e.g. max attempts reached and last one failed)
    if not final_validation_passed:
        # This case should ideally be caught by the exception in the loop, but as a safeguard:
        raise Exception(
            f"Pipeline did not complete successfully after {max_attempts} attempts. Last known feedback: {feedback_for_rerun}"
        )

    # If we broke out of the loop successfully, final_validation_passed is True
    # Proceed with data loading etc.
    print("Validation passed, proceeding with data loading.")

    # Ensure engine is the one from the successful attempt if re-initialization happens per loop.
    # In the current structure, engine is initialized inside the loop if table creation is attempted.
    # We need to make sure the `engine` used below is the correct one from the successful attempt.
    # The current logic re-initializes `engine` if schema execution succeeds. This should be fine.

    # Populate Tables with CSV Data
    lookup = push_data_in_db(engine, dataframe, table_order, table_traits)
    print("Data insertion complete!")

    total_rows = len(dataframe)

    # Count constraints from queries
    total_constraints_count = 0
    constraint_keywords = ["PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK"]
    if queries:
        for query_string in queries:  # queries is expected to be a list of SQL strings
            if isinstance(query_string, str):
                for keyword in constraint_keywords:
                    total_constraints_count += query_string.upper().count(
                        keyword.upper()
                    )

    for tbl_name in table_order:
        s = lookup.get(tbl_name, {}).get(
            "stats", {"attempt": 0, "dropped": 0}
        )  # Defensive get
        attempt = s.get("attempt", 0)
        dropped = s.get("dropped", 0)
        dropped_pct = (dropped / attempt) * 100 if attempt else 0
        print(
            f"{tbl_name}: dropped {dropped}/{attempt} rows ({dropped_pct:.1f}%) during load"
        )

    return {
        "engine": engine,
        "lookup": lookup,
        "table_order": table_order,
        "table_traits": table_traits,  # Pass along for consistency if needed
        "dataframe_rows": total_rows,  # Original number of rows in the input dataframe
        "total_constraints_count": total_constraints_count,  # Add the count here
        "queries": data_dict.get("queries", {}) if data_dict else {},
    }
