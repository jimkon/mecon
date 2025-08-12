import pandas as pd


def flatten_json_max_2d(json_input):
    json_output = {}
    for key, value in json_input.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                new_subkey = f"{key}.{subkey}"
                json_output[new_subkey] = subvalue
        else:
            json_output[key] = value
    return json_output


def normalise_df_column_names(df):
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    return df


def json_to_csv(json_input: list[dict]):
    list_fo_flat_dicts = [flatten_json_max_2d(_dict) for _dict in json_input]
    return pd.DataFrame.from_records(list_fo_flat_dicts)