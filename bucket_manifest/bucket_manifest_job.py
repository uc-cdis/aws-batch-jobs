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

import utils

logging.basicConfig(level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

NUMBER_OF_THREADS = 16
MAX_RETRIES = 10


def purge_queue(queue_url):
    client = boto3.client("sqs", region_name="us-east-1")
    try:
        client.purge_queue(QueueUrl=queue_url)
    except client.exceptions.QueueDoesNotExist as e:
        logging.error("Queue {} does not exist".format(queue_url))
        sys.exit(1)


def submit_job(job_queue, job_definition, key):
    """
    Submit job to the job queue

    Args:
        job_queue(str): job queue name
        job_definition(str): job difinition name
        key(str): S3 object key
    
    Returns:
        None
    """
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
            logging.info("submitting job to compute metadata of {}".format(key))
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
    List all objects in the bucket
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
        logging.error("Can not list objects in the bucket {}. Detail {}".format(bucket_name, e))

    return result


def write_messages_to_tsv(queue_url, n_total_messages):
    """
    Consume the sqs and write results to tsv manifest

    Args:
        queue_url(str): SQS url
        n_total_messages(int): The expected number of messages being received

    """
    # Create SQS client
    logging.info("Start consuming queue {}".format(queue_url))
    sqs = boto3.client("sqs", region_name="us-east-1")
    n_messages = 0
    while True:
        try:
            # recive a message from SQS queue
            response = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=["SentTimestamp"],
                MaxNumberOfMessages=1,
                VisibilityTimeout=0,
                WaitTimeSeconds=0,
            )
            n_messages += len(response["Messages"])
            for message in response["Messages"]:
                receipt_handle = message["ReceiptHandle"]
                # Delete received message from queue
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                logging.info(json.loads(message["Body"]))

            if n_messages >= n_total_messages:
                break

        except ClientError as e:
            logging.error(e)
        except KeyError:
            # Queue is empty. Check again later!!!
            logging.info("Queue is empty!")
            time.sleep(10)

    logging.info("DONE!!!")


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
        purge_queue(args.sqs)
        keys = list_objects(args.bucket)
        submit_jobs(args.job_queue, args.job_definition, keys)
        write_messages_to_tsv(args.sqs, len(keys))
