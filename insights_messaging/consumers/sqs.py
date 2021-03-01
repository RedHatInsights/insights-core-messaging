import logging
import os
import boto3

from botocore.exceptions import ClientError

from insights_messaging.consumers import Consumer

log = logging.getLogger(__name__)


class SQS(Consumer):
    def __init__(
        self,
        publisher,
        downloader,
        engine,
        redis,
        aws_access_key_env,
        aws_secret_access_key_env,
        **kwargs
    ):

        super().__init__(publisher, downloader, engine, redis)

        aws_access_key_id = os.getenv(aws_access_key_env, "AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv(
            aws_secret_access_key_env, "AWS_SECRET_ACCESS_KEY"
        )
        queue_url = kwargs.get("queue_url")

        self.client = boto3.client(
            "sqs",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        self.queue_url = queue_url if queue_url else os.getenv("QUEUE_URL")
        self.delete_message = kwargs.get("delete_message")

    def deserialize(self, bytes_):
        raise NotImplementedError()

    def handles(self, input_msg):
        return True

    def run(self):

        while True:
            try:
                messages = self.client.receive_message(QueueUrl=self.queue_url)
                if messages["Messages"] == []:
                    continue
            except ClientError as e:
                log.exception(e)
                continue

            for message in messages["Messages"]:
                try:
                    if self.handles(message):
                        self.process(message)
                except Exception as ex:
                    log.exception(ex)
                finally:
                    if self.delete_message:
                        self.client.delete(
                            QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                        )
