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
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

CHUNK_SIZE = os.environ.get("CHUNK_SIZE", 1024 * 1024 * 10)
ACCESS_KEY_ID = os.environ["ACCESS_KEY_ID"]
SECRET_ACCESS_KEY = os.environ["SECRET_ACCESS_KEY"]
SQS_NAME = os.environ["SQS_NAME"]
REGION = os.environ.get("REGION", "us-east-1")
BUCKET = os.environ["BUCKET"]
S3KEY = os.environ["KEY"]
MAX_RETRIES = 3

# a file containing a "guid" column and additional, arbitrary columns to populate
# into the metadata service


def compute_object_metadata(queue_name):
    """
    Compute s3 object metadata and send the output to sqs
    The bucket and the key are stored as environment variables that were submitted to the job queue


    Args:
        queue_name(str): SQS name
    
    Returns:
        None
    """

    md5_hash = hashlib.md5()
    s3Client = boto3.client(
        "s3", aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY
    )
    n_tries = 0

    output = {}
    while n_tries < MAX_RETRIES:
        try:
            response = s3Client.get_object(Bucket=BUCKET, Key=unquote_plus(S3KEY))
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
        except Exception as e:
            n_tries += 1
            if n_tries == MAX_RETRIES:
                output = {
                    "url": "s3://{}/{}".format(BUCKET, S3KEY),
                    "ERROR": "{}".format(e),
                }

        time.sleep(10 ** n_tries)

    sqs = boto3.resource("sqs", region_name=REGION)
    # Get the queue. This returns an SQS.Queue instance
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    # send msg
    n_tries = 0
    while n_tries < MAX_RETRIES:
        try:
            response = queue.send_message(MessageBody="{}".format(json.dumps(output)))
            break
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


if __name__ == "__main__":
    logging.info("SQS_NAME: {}".format(SQS_NAME))
    compute_object_metadata(SQS_NAME)
