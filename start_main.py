from make_parquet import ParquetMaker
from read_queue import read_queue_and_make_parquet
from concurrent import futures
from send_discord import send_discord_msg
import boto3

def read_queue(queue_param_dict):
    """queue 의 내용을 읽어서 gzip 으로 압축된 parquet로 만들어 반환합니다.

    Args:
        queue_name (str): 
        from_bucket_name (str): 
        from_bucket_prefix (str): 
        to_bucket_name (str): 
        to_bucket_prefix (str): 
    """
    queue_name = queue_param_dict['queue_name']
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
    
    return {
            'year': year,
            'month' : month,
            'day' : day
            }
   

while 1:
    queue_param_dict = {'queue_name' : "dev-cdata2-datatime.fifo",
                        'from_bucket_name' : "storelink-prod-fstore-src",
                        'from_bucket_prefix' : "cData_day2",
                        'to_bucket_name' : "storelink-data-etl-dev",
                        'to_bucket_prefix' : "cdata_day2_etl"}
    date_dict_list = []
    for i in range(1,3+1):
        date_dict = read_queue(queue_param_dict)
        date_dict_list.append(date_dict)

    thread_list= []
    for date_dict in date_dict_list:
        thread = ParquetMaker(  date_dict['year'],
                                date_dict['month'],
                                date_dict['day'],
                                queue_param_dict['from_bucket_name'] ,
                                queue_param_dict['from_bucket_prefix'], 
                                queue_param_dict['to_bucket_name'],
                                queue_param_dict['to_bucket_prefix'])
        thread_list.append(thread)
        thread.start()
    
    for thread in thread_list:
        thread.join()

        
send_discord_msg("""
        Parquet 변경 끝 !!
""")