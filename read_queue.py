import boto3

sqs_client = boto3.client('sqs',region_name='ap-northeast-2')
sqs_resource = boto3.resource('sqs',region_name='ap-northeast-2')

QUEUE_NAME = "dev-cdata2-datatime.fifo"
queue = sqs_resource.get_queue_by_name(QueueName=QUEUE_NAME)

while 1:
    
    response = sqs_client.receive_message(
        QueueUrl = queue.url,
        MaxNumberOfMessages = 1
    )
    try:
        date = response['Messages'][0]['Body']
    except KeyError:
        break
        
    message_handle = response['Messages'][0]['ReceiptHandle']

    year = date[:4]
    month = date[5:7]
    day = date[8:10]

    from make_parquet import make_parquet
    make_parquet(year,month,day)

    response = sqs_client.delete_message(QueueUrl=queue.url,
                                        ReceiptHandle=message_handle)
    breakpoint()

from send_discord import send_discord_msg
send_discord_msg("""
        Parquet 변경 끝 !!
""")