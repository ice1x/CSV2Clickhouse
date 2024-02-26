import os

HOST = "localhost"
PORT = 9123
USER = ""
PASSWORD = ""


CALL_STRING = (
    # "sed -i 's/,NULL,/,,/g' {file_path} &&"
    "cat {file_path} | "
    "curl 'http://{host}:{port}/?query=INSERT%20INTO%20%60{table_name}%60%20FORMAT%20CSVWithNames' --data-binary @-"
)


if __name__ == "__main__":
    import sys
    file_name = sys.argv[1]
    table_name = sys.argv[2]
    os.system(CALL_STRING.format(file_path=file_name, host=HOST, port=PORT, table_name=table_name))
