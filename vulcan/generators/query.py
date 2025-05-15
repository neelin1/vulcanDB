import vulcan.generators.metadata as vgm
import vulcan.utils.llm_helpers as vuo
import pandas as pd
from typing import Optional


def generate_sql_queries(
    dataframe: pd.DataFrame,
    single_table: bool,
    feedback_from_validation: Optional[str] = None,
):
    info = vgm.get_dataframe_description(dataframe)
    print(">> DATAFRAME DESCRIPTION", info)
    samples = vgm.get_dataframe_samples(dataframe, 30)
    # print("DATAFRAME SAMPLES", samples)
    data = {
        "raw_data": samples,
        "structure": info,
        "single_table": single_table,
    }

    data = vuo.generate_schema(data, feedback=feedback_from_validation)
    data = vuo.generate_table_list(data, feedback=feedback_from_validation)
    data = vuo.generate_table_traits(data, feedback=feedback_from_validation)
    data = vuo.generate_constraints(data, feedback=feedback_from_validation)
    data = vuo.generate_sql_queries(data, feedback=feedback_from_validation)

    return data
