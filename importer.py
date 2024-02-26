import os

from config import CH_SERVER_URL, CH_SERVER_PORT

CALL_STRING = (
    "cat {file_path} | "
    "curl 'http://{host}:{port}/?query=INSERT%20INTO%20%60{table_name}%60%20FORMAT%20CSVWithNames' --data-binary @-"
)


def replace_null(input_file):
    # TODO: Add error handling
    # Attention - side effect: this function modifies the input file
    awk_command = r"""awk 'BEGIN { FS = ","; OFS = "," } { for (i=1; i<=NF; i++)
    { if ($i == "NULL" && i != NF) { $i = "" } else if ($i == "NULL" && i == NF) { $i = "" "," } } print }'"""
    temp_file = input_file + '.tmp'
    command = f"{awk_command} {input_file} > {temp_file}"

    # TODO: Rewrite this to use subprocess.run
    os.system(command)
    os.rename(temp_file, input_file)


def import_csv_to_clickhouse(_file_name: str, _table_name: str) -> None:
    replace_null(_file_name)

    # TODO: Rewrite this to use subprocess.run
    os.system(CALL_STRING.format(file_path=_file_name, host=CH_SERVER_URL, port=CH_SERVER_PORT, table_name=_table_name))


if __name__ == "__main__":
    import sys
    file_name = sys.argv[1]
    table_name = sys.argv[2]
    import_csv_to_clickhouse(file_name, table_name)
