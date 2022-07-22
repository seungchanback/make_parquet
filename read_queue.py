"""
queue 의 내용을 읽어서 gzip 으로 압축된 parquet로 만들어 반환합니다.
"""

import boto3

from send_queue import send_queue
from make_parquet import ParquetMaker

def read_queue_and_make_parquet(queue_param_dict):
    """queue 의 내용을 읽어서 gzip 으로 압축된 parquet로 만들어 반환합니다.

    Args:
        queue_name (str): 
        from_bucket_name (str): 
        from_bucket_prefix (str): 
        to_bucket_name (str): 
        to_bucket_prefix (str): 
    """
    queue_name = queue_param_dict['queue_name']
    from_bucket_name = queue_param_dict['from_bucket_name']
    from_bucket_prefix = queue_param_dict['from_bucket_prefix']
    to_bucket_name = queue_param_dict['to_bucket_name']
    to_bucket_prefix = queue_param_dict['to_bucket_prefix']

    sqs_client = boto3.client('sqs',region_name='ap-northeast-2')
    sqs_resource = boto3.resource('sqs',region_name='ap-northeast-2')
    
    queue = sqs_resource.get_queue_by_name(QueueName=queue_name)
    
    response = sqs_client.receive_message(
        QueueUrl = queue.url,
        MaxNumberOfMessages = 1
    )
        
    
    try:
        date = response['Messages'][0]['Body']
    except KeyError:
        raise Exception("끝")
    
    year = date[:4]
    month = date[5:7]
    day = date[8:10]

    message_handle = response['Messages'][0]['ReceiptHandle']
    response = sqs_client.delete_message(QueueUrl=queue.url,
                                        ReceiptHandle=message_handle)

    print(f"{date} 변환 시작")

    try:
        parquet_maker = ParquetMaker(year,month,day)
        parquet_maker.make_parquet(
                from_bucket_name = from_bucket_name,
                from_bucket_prefix = from_bucket_prefix,
                to_bucket_name = to_bucket_name,
                to_bucket_prefix = to_bucket_prefix 
        )
    except:
        send_queue(f"{to_bucket_name}-{to_bucket_prefix}-fail.fifo",f"{year}{month}{day}")


