from sys import prefix
import boto3
import pandas as pd
import s3fs
from tqdm import tqdm
from datetime import datetime
import string

from send_discord import send_discord_msg



RAW_BUCKET_NAME = 'storelink-prod-fstore-src'
RAW_BUCKET_PREFIX = string.Template("$prefix/$year/$month/$day")
OUTPUT_BUCKET_NAME = 'storelink-data-etl-dev'
OUTPUT_BUCKET_PREFIX = string.Template("$prefix/$year/$month/$day")

SCHEMA_CHECK_BUCKET = 'storelink-data-etl-dev'
SCHEMA_CHECK_YEAR = "2021"
SCHEMA_CHECK_MONTH = "09"
SCHEMA_CHECK_DAY = "18"
SCHEMA_CHECK_BUCKET_PRIFIX = f"cData_day2_etl/{SCHEMA_CHECK_YEAR}/{SCHEMA_CHECK_MONTH}/{SCHEMA_CHECK_DAY}/{SCHEMA_CHECK_YEAR}-{SCHEMA_CHECK_MONTH}-{SCHEMA_CHECK_DAY}.parquet.gz"

def _make_daily_dataFrame(obj_list, bucket_name):
    def _generate_str_dataframe(dataFrame):

        str_dataFrame = pd.DataFrame()

        for column in dataFrame.columns:
            str_dataFrame[column] = dataFrame[column].astype("string")

        return str_dataFrame

    try:
        key_list = [content['Key'] for content in obj_list['Contents']]
    except KeyError:

        return None

    total_temp_dataFrame = None
    
    for index, key in tqdm(enumerate(key_list), total=len(key_list)):
        key = f"s3://{bucket_name}/{key}"

        if index == 0 :
            total_temp_dataFrame = _generate_str_dataframe(pd.read_csv(key))
        elif index > 0 :
            temp_dataFrame = _generate_str_dataframe(pd.read_csv(key))
            total_temp_dataFrame = pd.concat([total_temp_dataFrame,temp_dataFrame], axis=0)

    return total_temp_dataFrame


def make_parquet(year, month, day):
    S3 = boto3.client('s3')

    bucket_name = RAW_BUCKET_NAME

    #year = target_date.strftime("%Y")
    #month = target_date.strftime("%m")
    #day = target_date.strftime("%d")

    bucket_prefix = RAW_BUCKET_PREFIX.substitute(prefix = 'cData_day2',
                                                year = year,
                                                month = month,
                                                day = day)

    obj_list = S3.list_objects(Bucket=bucket_name, Prefix=bucket_prefix)
    
    daily_dataFrame = _make_daily_dataFrame(obj_list,bucket_name)

    output_bucket_prefix = OUTPUT_BUCKET_PREFIX.substitute(prefix = 'cData_day2_etl',
                                                            year = year,
                                                            month = month,
                                                            day = day)

    output_dir_path = f"s3://{OUTPUT_BUCKET_NAME}/{output_bucket_prefix}"

    # if not os.path.exists(output_dir_path):
    #     os.makedirs(output_dir_path, exist_ok=True)
    
    output_filepath = f"{output_dir_path}/{year}-{month}-{day}.parquet.gz"
    if type(daily_dataFrame) != type(None):
        schema_check_dataFrame = pd.read_parquet(f"./2021-09-18.parquet.gz")
        if list(schema_check_dataFrame.columns) != list(daily_dataFrame):
            send_discord_msg(f"""
            Parquet 변경
            - {year}-{month}-{day} 날짜는 스키마가 다릅니다.""")

        daily_dataFrame.to_parquet(path=output_filepath, compression="gzip")
    elif type(daily_dataFrame) == type(None):
        send_discord_msg(f"""
        Parquet 변경
        - BUCKET : {RAW_BUCKET_NAME}
        - {year}-{month}-{day} 날짜는 데이터가 없습니다.""")

