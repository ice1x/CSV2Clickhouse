"""
% curl -X POST 'http://localhost:18123/' -d '
CREATE TABLE my_table (
    ride_id String,
    rideable_type String,
    started_at DateTime,
    ended_at DateTime,
    start_station_name String,
    start_station_id String,
    end_station_name String,
    end_station_id String,
    start_lat Float64,
    start_lng Float64,
    end_lat Float64
) ENGINE = Log'
"""


"""
% curl -X POST 'http://localhost:18123/' -d '
CREATE TABLE my_table (
    ride_id String,
    rideable_type String,
    started_at DateTime,
    ended_at DateTime,
    start_station_name String,
    start_station_id String,
    end_station_name String,
    end_station_id String,
    start_lat Float64,
    start_lng Float64,
    end_lat Float64
) ENGINE = MergeTree ORDER BY started_at'
"""


"""
% cat JC-202311-citibike-tripdata.csv | curl 'http://localhost:18123/?query=INSERT%20INTO%20my_table%20FORMAT%20CSV' --data-binary @-
"""
import zipfile
from pathlib import Path

import boto3
import pandas as pd

BUCKET = "citibike-tripdata"

# s3_fs = s3fs.S3FileSystem(s3_additional_kwargs={'ServerSideEncryption': 'AES256'})


def _zipped_csv_from_s3_to_df(bucket, s3_fs):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    with s3_fs.open(path) as zipped_dir:
        with zipfile.ZipFile(zipped_dir, mode='r') as zipped_content:
            for score_file in zipped_content.namelist():
                with zipped_content.open(score_file) as scores:
                    return pd.read_csv(scores)


# def _zipped_csv_from_s3_to_df(path, s3_fs):
#     with s3_fs.open(path) as zipped_dir:
#         with zipfile.ZipFile(zipped_dir, mode='r') as zipped_content:
#             for score_file in zipped_content.namelist():
#                 with zipped_content.open(score_file) as scores:
#                     return pd.read_csv(scores)


market_score = _zipped_csv_from_s3_to_df(BUCKET)

