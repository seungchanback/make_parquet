import boto3

def send_queue(queue_name : str, message : str):
    sqs_client = boto3.client('sqs')
    sqs_resource = boto3.resource('sqs')
    """Queue 이름 확인 후 없으면 생성하고, message 를 큐에 전송합니다.

    Args:
        queue_name (str): message 를 전송할 큐 이름
        message (str): message
    """

    today_queue = None

    try:
        today_queue = sqs_resource.get_queue_by_name(QueueName= queue_name)
    except sqs_client.exceptions.QueueDoesNotExist:
        
        sqs_resource.create_queue(  QueueName = queue_name,
                                    Attributes= {   'FifoQueue' : 'true',
                                                    'ContentBasedDeduplication' : 'true'})
        today_queue = sqs_resource.get_queue_by_name(QueueName=queue_name)
    
    sqs_client.send_message(QueueUrl=today_queue.url,
                            MessageBody=message,
                            MessageGroupId=queue_name
                            )
    return None


