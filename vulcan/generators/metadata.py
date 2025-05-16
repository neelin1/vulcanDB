from io import StringIO

import pandas as pd


def abbreviate_long_strings(value: str, max_length: int = 500) -> str:
    """
    Abbreviates a string if it exceeds max_length, appending a summary of the abbreviation.
    """
    if not isinstance(value, str):
        return value  # Return non-string values as is
    if len(value) > max_length:
        remaining_chars = len(value) - max_length
        return f"{value[:max_length]}...(abbreviated, string continues for {remaining_chars} more characters)"
    return value


def get_dataframe_description(dataframe: pd.DataFrame) -> str:
    """
    Generates a formatted string describing the DataFrame's columns, non-null values, and data types.

    Parameters:
    - dataframe: The DataFrame to describe.

    Returns:
    - A formatted string with the description of DataFrame.
    """
    # Capture the DataFrame info in a string buffer
    buffer = StringIO()
    dataframe.info(buf=buffer)
    info = buffer.getvalue()
    buffer.close()

    # Extract and format the column information
    lines = info.split("\n")
    column_info_lines = [line for line in lines if line.strip()]
    # Prepare the formatted output
    formatted_output = "Column             Non-Null             Dtype\n"
    formatted_output += "-" * 40 + "\n"
    for line in column_info_lines[4:]:
        try:
            parts = line.strip().split()
            column_name = parts[1]
            non_null_count = parts[3]
            dtype = parts[4]
            formatted_output += f"{column_name:20} {non_null_count:15} {dtype}\n"
        except IndexError as _e:
            continue
    return formatted_output


def get_dataframe_samples(
    dataframe: pd.DataFrame, sample_size: int = 10, max_length: int = 200
) -> str:
    """
    Returns a string representation of a sample from the DataFrame.

    Parameters:
    dataframe : pd.DataFrame
        The DataFrame to sample from.
    sample_size : int, default 10
        Number of rows to include in the sample.
    max_length : int, default 500
        Maximum allowed length for string values before abbreviation.

    Returns:
    - A string representation of the DataFrame sample.
    """
    if dataframe.empty:
        return "DataFrame is empty."

    sample = dataframe.sample(n=min(sample_size, len(dataframe))).copy()

    for col in sample.select_dtypes(include=["object"]).columns:
        sample[col] = sample[col].apply(
            lambda val: abbreviate_long_strings(val, max_length)
        )

    return sample.to_string(index=False)
