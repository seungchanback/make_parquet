import boto3

sqs_client = boto3.client('sqs',region_name='ap-northeast-2')
sqs_resource = boto3.resource('sqs',region_name='ap-northeast-2')


bodys = []
queue_name = 'storelink-data-etl-dev-cdata_day2_etl-success.fifo'
queue = sqs_resource.get_queue_by_name(QueueName=queue_name)

while True:
    resp = sqs_client.receive_message(
        QueueUrl=queue.url,
        MaxNumberOfMessages=10
    )

    try:
        messages = resp['Messages']
    except KeyError:
        print('No messages on the queue!')
        break


    entries = [
        {   'Id': msg['MessageId'],
            'ReceiptHandle': msg['ReceiptHandle']   }
        for msg in resp['Messages']
    ]
    temp_bodys = [message['Body'] for message in resp['Messages']]
    bodys = bodys + temp_bodys
    
    result = sqs_client.delete_message_batch(
        QueueUrl=queue.url,
        Entries=entries)
    print(bodys)
    print(result)

print("hello")
for success_date in bodys:
    if success_date in origin_dates:
        origin_dates.remove(success_date)
origin_dates = [date.strftime("%Y%m%d") for date in origin]
for date in origin_dates:
    send_queue(queue_name, date)
    