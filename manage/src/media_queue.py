import boto3
import os
from constants import QUEUE_NAME, QUEUE_VISIBILITY
from init import logger


def create_queue(
    queue_name: str = QUEUE_NAME, queue_visibility: str = QUEUE_VISIBILITY
) -> bool:
    """ Create SQS queue """

    logger.info("Creating queue...")

    sqs = boto3.client("sqs")

    try:
        queues = sqs.list_queues(QueueNamePrefix=queue_name)
        queue_url = queues["QueueUrls"][0]
        print(queue_url)
        # print(queues['VisibilityTimeout'])
        logger.debug(queues)
    except Exception as e:
        logger.error(e)
        raise

    if not queues:
        try:
            queue = sqs.create_queue(
                QueueName=queue_name,
                Attributes={"VisibilityTimeout": queue_visibility},
            )
            logger.debug(queue)
        except Exception as e:
            logger.error(e)
            raise
    else:
        logger.warning(f"Queue {queue_name} exists! Skipping queue creation.")
        return False

    logger.info("...queue successfully created.")

    return True


def send_to_queue(message: str, queue_name: str = QUEUE_NAME) -> str:
    """ Send message to SQS queue """

    assert message is not None
    assert type(message) == str

    logger.info("Sending task to queue...")

    try:
        sqs = boto3.client("sqs")
        queues = sqs.list_queues(QueueNamePrefix=queue_name)
        queue_url = queues["QueueUrls"][0]
        response = sqs.send_message(QueueUrl=queue_url, MessageBody=message)
        logger.debug(response)
    except Exception as e:
        logger.error(e)
        raise

    logger.info("...task successfully sent to queue.")

    return response


def queue_count(queue_name: str = QUEUE_NAME) -> int:
    """ Display the number of messages in SQS queue """

    logger.info("Getting queue messages count...")
    try:
        sqs = boto3.client("sqs")
        queues = sqs.list_queues(QueueNamePrefix=queue_name)
        queue_url = queues["QueueUrls"][0]

        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
            ],
        )
        logger.debug(response)
    except Exception as e:
        logger.error(e)
        raise

    messages_available = int(
        response["Attributes"]["ApproximateNumberOfMessages"]
    )
    messages_in_flight = int(
        response["Attributes"]["ApproximateNumberOfMessagesNotVisible"]
    )

    logger.info("...done")
    logger.debug(f"messages available: {messages_available}")
    logger.debug(f"messages in flight: {messages_in_flight}")

    return messages_in_flight
