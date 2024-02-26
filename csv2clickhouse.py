from convert import create_table
from importer import import_csv_to_clickhouse


def csv2clickhouse(_file_name: str, _table_name: str) -> None:
    # TODO: Add error handling
    create_table(f"`{_table_name}`", _file_name)
    import_csv_to_clickhouse(_file_name, _table_name)


if __name__ == "__main__":
    import sys
    from pathlib import Path

    file_name = sys.argv[1]
    table_name = Path(file_name).name
    csv2clickhouse(file_name, table_name)
