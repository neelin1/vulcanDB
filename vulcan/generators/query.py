import vulcan.generators.metadata as vgm
import vulcan.utils.openai as vuo


def generate_sql_queries(dataframe: str, db_type: str):
    info = vgm.get_dataframe_description(dataframe)
    print(">> DATAFRAME DESCRIPTION", info)
    samples = vgm.get_dataframe_samples(dataframe, 30)
    # print("DATAFRAME SAMPLES", samples)
    data = {"database": db_type, "raw_data": samples, "structure": info}

    data = vuo.generate_schema(data)
    data = vuo.generate_alias_mapping(data)
    data = vuo.generate_constraints(data)
    data = vuo.generate_sql_queries(data)

    return data
