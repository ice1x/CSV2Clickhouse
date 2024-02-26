import clickhouse_connect
import pandas as pd

from pandas import DataFrame

from config import CH_SERVER_URL, CH_SERVER_PORT


PD_TYPE_MAPPING = {"int64": "UInt64", "float64": "Float64"}
CH_CLIENT = clickhouse_connect.get_client(host=CH_SERVER_URL, port=CH_SERVER_PORT)


def get_types(df: DataFrame) -> dict[str, str]:
    object_types = {}
    for i in df:
        key_type = str(df[i].dtype)
        if key_type in PD_TYPE_MAPPING:
            object_types[i] = PD_TYPE_MAPPING[key_type]
        else:
            try:
                pd.to_datetime(df[i][0])
                object_types[i] = "DateTime"
            except ValueError:
                object_types[i] = "String"
    return object_types


def quote(s: str) -> str:
    return f"`{s}`" if " " in s else s


def create_table(table_name: str, file_name: str) -> None:
    # TODO: Add error handling - table exists
    df = pd.read_csv(file_name, nrows=1)
    metadata = ", ".join([f"{quote(k)} {v}" for k, v in get_types(df).items()])
    if not metadata:
        raise ValueError("No columns found")

    CH_CLIENT.command(f"CREATE TABLE {table_name} ({metadata}) ENGINE MergeTree ORDER BY tuple()")


if __name__ == "__main__":
    import sys
    from pathlib import Path

    file_name = sys.argv[1]
    create_table(f"`{Path(file_name).name}`", file_name)
