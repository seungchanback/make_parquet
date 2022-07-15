import boto3
import pandas as pd

sqs_client = boto3.client('sqs')
sqs_resource = boto3.resource('sqs')

queue_name = f"dev-cdata2-datatime.fifo"
today_queue = None

#dev-cdata2-datatime-sample : 개발용

# Queue 존재 확인
try:
    today_queue = sqs_resource.get_queue_by_name(QueueName= queue_name)
except sqs_client.exceptions.QueueDoesNotExist:
    
    create_queue_result = sqs_resource.create_queue(QueueName = queue_name,
                                                    Attributes={'FifoQueue' : 'true',
                                                                'ContentBasedDeduplication' : 'true'})
    today_queue = sqs_resource.get_queue_by_name(QueueName=queue_name)

date_range = pd.date_range(start="2021-09-18",
                            end="2022-07-12")

for date in date_range:
    print(date)
    sqs_client.send_message(QueueUrl=today_queue.url,
                            MessageBody=str(date),
                            MessageGroupId=queue_name
                            )

