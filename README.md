# CSV2Clickhouse import

- Get table schema from CSV headers
- Create Clickhouse table according to discovered schema
- Replace ",NULL," by ",," if it is exist (modifies file)
- Import CSV file to newely created table


Tested on citibyke tripdata from https://s3.amazonaws.com/tripdata/index.html

# Call example:
```
python csv2clickhouse.py ./JC-202311-citibike-tripdata.csv
```
```
python csv2clickhouse.py ./201306-citibike-tripdata.csv
```

# Known issues:
- No error handling such like - existent table
- Non prod-ready code quality - stage: snippet
