    # """
    # 월을 입력받아서, 해당 월의 스키마가 이상한지 확인
    # """

import boto3
import string
import pandas as pd

TARGET_BUCKET_NAME =  'storelink-data-etl-dev'
TARGET_BUCKET_PREFIX = string.Template("$prefix/$year/$month/$day")

year = None
month = None

SCHEMA_CHECK_BUCKET = 'storelink-data-etl-dev'
SCHEMA_CHECK_YEAR = "2022"
SCHEMA_CHECK_MONTH = "01"
SCHEMA_CHECK_DAY = "18"
SCHEMA_CHECK_BUCKET_PRIFIX = f"cData_day2_etl/{SCHEMA_CHECK_YEAR}/{SCHEMA_CHECK_MONTH}/{SCHEMA_CHECK_DAY}/{SCHEMA_CHECK_YEAR}-{SCHEMA_CHECK_MONTH}-{SCHEMA_CHECK_DAY}.parquet.gz"

SCHEMA_DATAFRAME = pd.read_parquet(f"s3://{SCHEMA_CHECK_BUCKET}/{SCHEMA_CHECK_BUCKET_PRIFIX}")

date_range = pd.date_range(start="2022-02-26",end="2022-02-27")

wrong_date_list = []
for date in date_range:
    
    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")
    target_bucket_prefix = TARGET_BUCKET_PREFIX.substitute(prefix='cData_day2_etl',
                                    year=year,
                                    month=month,
                                    day=day)
    target_pd = pd.read_parquet(f"s3://{TARGET_BUCKET_NAME}/{target_bucket_prefix}/{year}-{month}-{day}.parquet.gz")

    schema_check_columns = list(SCHEMA_DATAFRAME.columns)
    target_pd_columns = list(target_pd.columns)

    schema_check_columns.sort()
    target_pd_columns.sort()

    if schema_check_columns != target_pd_columns:
        print(f"{year}-{month}-{day} 의 스키마는 이상합니다.")
        wrong_date_list.append(f"{year}-{month}-{day}")
    else:
        print(f"{year}-{month}-{day} 는 정상입니다.")

print("----- 이상한 날짜 목록 ----------")
for i in wrong_date_list:
    print(f"{i}\n")
