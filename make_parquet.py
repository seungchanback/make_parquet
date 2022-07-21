"""
CSV 를 Parquet 로 만들어주는 모듈

"""
from datetime import datetime

import s3fs
import boto3
import pandas as pd
from tqdm import tqdm

from send_queue import send_queue
from send_discord import send_discord_msg


class ParquetMaker():
    
    def __init__(self, year, month, day):
        self.s3_client = boto3.client('s3')
        self.year = year
        self.month = month
        self.day = day

    
    def _make_daily_dataframe(self, key_list : list, bucket_name : str) -> pd.DataFrame:
        """입력된 key_list 에 해당하는 csv 를 모아서 하나의 dataframe 으로 반환합니다.
        이 떄, dataframe의 모든 컬럼의 타입은 string 입니다.

        Args:
            key_list (list): _description_
            bucket_name (str): _description_

        Returns:
            pd.DataFrame: _description_
        """
        def _generate_str_dataframe(temp_dataframe : pd.DataFrame) -> pd.DataFrame:
            """입력한 dataframe 의 모든 컬럼을 string 타입으로 변환 후 반환합니다.

            Args:
                dataframe (pd.DataFrame): 입력 데이터프레임

            Returns:
                str_dataFrame : string 으로 변환된 데이터프레임
            """
            str_dataframe = pd.DataFrame()
            for column in temp_dataframe.columns:
                str_dataframe[column] = temp_dataframe[column].astype("string")

            return str_dataframe

        total_daily_dataframe = None
        
        for index, key in tqdm(enumerate(key_list), total=len(key_list)):
            key = f"s3://{bucket_name}/{key}"

            try:
                temp_csv = pd.read_csv(key)
            except IndexError:
                print(f"{key} 에 대한 csv 파일을 만들 수 없습니다.")
                continue
                

            if index == 0 :
                total_daily_dataframe = _generate_str_dataframe(temp_csv)
            elif index > 0 :
                temp_dataFrame = _generate_str_dataframe(temp_csv)
                total_daily_dataframe = pd.concat([total_daily_dataframe,temp_dataFrame], axis=0)
                
        return total_daily_dataframe

    def _get_key_list(self, from_bucket_name : str,from_bucket_prefix : str):
        
        s3_client = self.s3_client
        paginator = s3_client.get_paginator('list_objects_v2')
        key_list = list()

        response_iterator = paginator.paginate(
            Bucket=from_bucket_name,
            Prefix=f"{from_bucket_prefix}/{self.year}/{self.month}/{self.day}"
        )

        for page in response_iterator:
            try:
                key_list = key_list + [content['Key'] for content in page['Contents']]
            except KeyError:
                return key_list
        
        return key_list

    def _check_none(self, daily_dataframe : pd.DataFrame):
        """ dataframe 에 타입이 None 인지 확인합니다.

        Args:
            daily_dataframe (pd.DataFrame): 대상 데이터프레임

        Returns:
            None
        """
        
        if type(daily_dataframe) == type(None):
            send_discord_msg(f"""
            Parquet 변경
            - {self.year}-{self.month}-{self.day} 날짜는 데이터가 없습니다.""")
            raise Exception("스키마가 충돌이 발생하였습니다..")
        
        return None

    
    def _check_schema(self, daily_dataframe : pd.DataFrame):
        """ 입력된 dataframe 과 schema_check 폴더의 parquet 파일과 스키마를 비교합니다.


        Args:
            daily_dataframe (pd.DataFrame): 비교 대상 데이터프레임
        """

        def _get_criteria_schema(schema_parquet_path : str) -> list:
            """schema_parquet_path 를 입력받아 스키마 파일의 정렬된 Column 을 리스트로 반환합니다.

            Args:
                schema_parquet_path (str): 스키마 파일 경로

            Returns:
                schema_columns(list): 스키마 파일의 정렬된 컬럼 리스트
            """
            schema_dataframe = pd.read_parquet(schema_parquet_path)
            schema_columns = schema_dataframe.columns.to_list()
            schema_columns.sort()

            return schema_columns

        criteria_schema = _get_criteria_schema("./schema_check/cdata_day2_etl/schema.parquet")
        from_daily_dataframe_schema = daily_dataframe.columns.to_list()
        from_daily_dataframe_schema.sort()

        if criteria_schema != from_daily_dataframe_schema:
            send_discord_msg(f"""
                Parquet 변경
                - {self.year}-{self.month}-{self.day} 날짜는 스키마가 다릅니다.""")
            raise Exception("스키마가 충돌이 발생하였습니다..")
        
        return None

    def make_parquet(self,
                    from_bucket_name : str,
                    from_bucket_prefix : str,
                    to_bucket_name : str,
                    to_bucket_prefix : str):
    

        from_bucket_key_list = self._get_key_list(from_bucket_name,from_bucket_prefix)
        daily_dataframe = self._make_daily_dataframe(from_bucket_key_list, from_bucket_name)
        self._check_schema(daily_dataframe)

        output_path = f"s3://{to_bucket_name}/{to_bucket_prefix}/{self.year}/{self.month}/{self.day}/{self.year}-{self.month}-{self.day}.parquet.gz"
        daily_dataframe.to_parquet(path=output_path, compression="gzip")
        
        send_queue(f"{self.year}{self.month}{self.day}-success.fifo",f"{self.year}{self.month}{self.day}")
