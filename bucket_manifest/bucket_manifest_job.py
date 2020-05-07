import sys
import boto3
import json
import time
from functools import partial
import argparse
import botocore
import logging
from multiprocessing.pool import Pool

from botocore.exceptions import ClientError

logging.basicConfig(level=logging.DEBUG)
logging.getlogging().addHandler(logging.StreamHandler(sys.stdout))

NUMBER_OF_THREADS = 16
MAX_RETRIES = 10


def submit_job(job_queue, job_definition, key):
    client = boto3.client("batch", region_name="us-east-1")
    n_tries = 0

    while n_tries < MAX_RETRIES:
        try:
            client.submit_job(
                jobName="test",
                jobQueue=job_queue,
                jobDefinition=job_definition,
                containerOverrides={"environment": [{"value": key, "name": "KEY"}]},
            )
            break
        except ClientError as e:
            if e.response["Error"]["Code"] == "AccessDeniedException":
                logging.error(e)
                break
            if e.response["Error"]["Code"] != "TooManyRequestsException":
                n_tries += 1
                logging.info("{}. Retry {}".format(e, n_tries))
            else:
                logging.info("TooManyRequestsException. Sleep and retry...")

        time.sleep(2 ** n_tries)


def submit_jobs(job_queue, job_definition, keys):
    par_submit_job = partial(submit_job, job_queue, job_definition)
    with Pool(NUMBER_OF_THREADS) as pool:
        pool.map(par_submit_job, keys)


def list_objects(bucket_name):
    """
    build object dataset for lookup with key is s3 object key and value contains
    storage class, size and md5
    to avoid list object operations
    """
    result = []

    with open("./creds.json") as creds_file:
        creds = json.load(creds_file)
        aws_access_key_id = creds.get("aws_access_key_id")
        aws_secret_access_key = creds.get("aws_secret_access_key")

    client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    try:
        paginator = client.get_paginator("list_objects_v2")
        print("start to list objects in {}".format(bucket_name))
        pages = paginator.paginate(Bucket=bucket_name, RequestPayer="requester")
        for page in pages:
            for obj in page["Contents"]:
                result.append(obj["Key"])
    except botocore.exceptions.ClientError as e:
        logging.error("Can not detect the bucket {}. Detail {}".format(bucket_name, e))

    return result


def write_message_to_tsv(queue_url, total_message):
    # Create SQS client
    sqs = boto3.client("sqs", region_name="us-east-1")
    n_messages = 0
    while True:
        try:
            # n_messagesReceive message from SQS queue
            response = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=["SentTimestamp"],
                MaxNumberOfMessages=1,
                MessageAttributeNames=["All"],
                VisibilityTimeout=0,
                WaitTimeSeconds=0,
            )
            n_messages += len(response["Messages"])
            for message in response["Messages"]:
                receipt_handle = message["ReceiptHandle"]
                # Delete received message from queue
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                print(message)

            if n_messages >= total_message:
                break

        except ClientError as e:
            logging.error(e)
        except KeyError:
            # Queue is empty. Check again later!!!
            logging.info("Queue is empty!")
            time.sleep(10)
            pass


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    bucket_manifest_cmd = subparsers.add_parser("bucket_manifest")
    bucket_manifest_cmd.add_argument("--bucket", required=True)
    bucket_manifest_cmd.add_argument("--job_queue", required=True)
    bucket_manifest_cmd.add_argument("--job_definition", required=True)
    bucket_manifest_cmd.add_argument("--sqs", required=True)
    bucket_manifest_cmd.add_argument("--out-bucket", required=True)

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    if args.action == "bucket_manifest":
        keys = list_objects(args.bucket)
        submit_jobs(args.job_queue, args.job_definition, keys)
        write_message_to_tsv(args.sqs, len(keys))

    # keys = list_objects("giangb-bucket-manifest-test")
    # submit_jobs(keys)
    # write_message_to_tsv("https://sqs.us-east-1.amazonaws.com/707767160287/terraform-example-queue", 1000)
