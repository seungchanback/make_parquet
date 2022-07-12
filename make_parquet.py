import boto3
import pandas as pd
import s3fs
from tqdm import tqdm
from datetime import datetime
import os



def _make_daily_dataFrame(obj_list, bucket_name):
    def _generate_str_dataframe(dataFrame):

        str_dataFrame = pd.DataFrame()

        for column in dataFrame.columns:
            str_dataFrame[column] = dataFrame[column].astype("string")

        return str_dataFrame


    key_list = [content['Key'] for content in obj_list['Contents']]
    total_temp_dataFrame = None
    
    for index, key in tqdm(enumerate(key_list), total=len(key_list)):
        key = f"s3://{bucket_name}/{key}"

        if index == 0 :
            total_temp_dataFrame = _generate_str_dataframe(pd.read_csv(key))
        elif index > 0 :
            temp_dataFrame = _generate_str_dataframe(pd.read_csv(key))
            total_temp_dataFrame = pd.concat([total_temp_dataFrame,temp_dataFrame], axis=0)

    return total_temp_dataFrame


def make_parquet(target_date):
    S3 = boto3.client('s3')
    bucket_name = 'storelink-prod-fstore-src'
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")

    bucket_prefix = f'cData_day_naver_pay2/{year}/{month}/{day}/'
    obj_list = S3.list_objects(Bucket=bucket_name, Prefix=bucket_prefix)
    
    daily_dataFrame = _make_daily_dataFrame(obj_list,bucket_name)
    output_dir_path = f"cdata_day_naverpay_etl/{year}/{month}/{day}"

    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path, exist_ok=True)
    
    output_filepath = f"{output_dir_path}/{year}-{month}-{day}.parquet.gz"
    if type(daily_dataFrame) != type(None):
        daily_dataFrame.to_parquet(path=output_filepath, compression="gzip")

from concurrent import futures
DATE_RANGE = pd.date_range(start="2021-09-24",end="2022-07-12")

with futures.ThreadPoolExecutor(max_workers=5) as executor :
    _ = [executor.submit(make_parquet,target_date) for target_date in DATE_RANGE]