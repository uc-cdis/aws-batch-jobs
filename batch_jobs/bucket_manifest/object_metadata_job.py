"""
Module for computing s3 object metadata.
"""
import sys
import os
import time
import boto3
import logging
import hashlib
import json
from urllib.parse import unquote_plus
from botocore.exceptions import ClientError


logging.basicConfig(level=logging.INFO)
# logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

CHUNK_SIZE = os.environ.get("CHUNK_SIZE", 1024 * 1024 * 10)
ACCESS_KEY_ID = os.environ.get("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN")
SQS_NAME = os.environ.get("SQS_NAME")
REGION = os.environ.get("REGION", "us-east-1")
BUCKET = os.environ.get("BUCKET")
S3KEY = os.environ.get("KEY")
MAX_RETRIES = 3


def run_job():
    """
    Run the job to compute object metadata.
    The bucket and the key are stored as environment variables.
    """
    output = compute_object_metadata()
    send_message(SQS_NAME, output)


def compute_object_metadata():
    """
    Compute s3 object metadata and send the output to sqs
    The bucket and the key are stored as environment variables that were submitted to the job queue
    """

    md5_hash = hashlib.md5()
    n_tries = 0

    s3Client = boto3.client(
        "s3",
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
    )

    output = {}
    while n_tries < MAX_RETRIES:
        try:
            response = s3Client.get_object(Bucket=BUCKET, Key=unquote_plus(S3KEY), RequestPayer='requester')
            res = response["Body"]
            data = res.read(CHUNK_SIZE)
            while data:
                md5_hash.update(data)
                data = res.read(CHUNK_SIZE)
            output = {
                "url": "s3://{}/{}".format(BUCKET, S3KEY),
                "md5": md5_hash.hexdigest(),
                "size": response["ContentLength"],
            }
            break
        except ClientError as e:
            if e.response["Error"]["Code"] == "AccessDeniedException":
                output = {"ERROR": "AccessDeniedException"}
                logging.error(e)
                break
            if e.response["Error"]["Code"] != "TooManyRequestsException":
                n_tries += 1
                if n_tries == MAX_RETRIES:
                    output = {
                        "url": "s3://{}/{}".format(BUCKET, S3KEY),
                        "ERROR": "{}".format(e),
                    }
                logging.info("{}. Retry {}".format(e, n_tries))
            else:
                logging.info("TooManyRequestsException. Sleep and retry...")
                time.sleep(10 ** n_tries)

        except Exception as e:
            n_tries += 1
            if n_tries == MAX_RETRIES:
                output = {
                    "url": "s3://{}/{}".format(BUCKET, S3KEY),
                    "ERROR": "{}".format(e),
                }
            time.sleep(1 ** n_tries)

    return output


def send_message(queue_name, msg_body):
    """
    send a message to sqs

    Args:
        queue_name(str): SQS name
        msg_body(dict): message content

    Returns:
        bool: True if the message was sent successfully
    """

    sqs = boto3.resource("sqs", region_name=REGION)
    # Get the queue. This returns an SQS.Queue instance
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    # send msg
    n_tries = 0
    while n_tries < MAX_RETRIES:
        try:
            queue.send_message(MessageBody="{}".format(json.dumps(msg_body)))
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "AccessDeniedException":
                logging.error(e)
                break
            if e.response["Error"]["Code"] != "TooManyRequestsException":
                n_tries += 1
                logging.info("{}. Retry {}".format(e, n_tries))
            else:
                logging.info(
                    "TooManyRequestsException (Send message to queue). Sleep and retry..."
                )

            time.sleep(2 ** n_tries)
    return False
