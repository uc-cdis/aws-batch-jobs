### Kube setup job needs to setup the right stuff for job def


"""
Module for submitting jobs to job queue and consuming SQS to generate a bucket manifest
"""
import os
import sys
import time
from datetime import datetime
import json
import csv
from functools import partial
import logging
from multiprocessing.pool import Pool

from urllib.parse import urlparse
import boto3
from botocore.exceptions import ClientError

from ..utils import utils

logging.basicConfig(level=logging.INFO)
# logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

NUMBER_OF_THREADS = 16
MAX_RETRIES = 10

REGION = os.environ.get("REGION", "us-east-1")


def run_job(bucket, job_queue, job_definition, sqs, out_bucket):
    """
    Start to run an job to generate bucket manifest
    Args:
        bucket(str): bucket name
        job_queue(str): job queue name
        job_definition(str): job definition name
        sqs(str): SQS url
        out_bucket(str): the bucket which the manifest is saved to

    Returns:
        bool: True if the job was submitted successfully
    """
    purge_queue(sqs)
    keys = list_objects(bucket)
    submit_jobs(job_queue, job_definition, keys)
    write_messages_to_tsv(sqs, len(keys), out_bucket)


def purge_queue(queue_url):
    """
    Remove all messages in the SQS queue
    """
    client = boto3.client("sqs", region_name=REGION)
    try:
        client.purge_queue(QueueUrl=queue_url)
    except client.exceptions.QueueDoesNotExist:
        logging.error("Queue {} does not exist".format(queue_url))
        sys.exit(1)


def submit_job(job_queue, job_definition, key):
    """
    Submit job to the job queue

    Args:
        job_queue(str): job queue name
        job_definition(str): job definition name
        key(str): S3 object key

    Returns:
        bool: True if the job was submitted successfully
    """
    client = boto3.client("batch", region_name=REGION)
    n_tries = 0

    while n_tries < MAX_RETRIES:
        try:
            client.submit_job(
                jobName="bucket_manifest",
                jobQueue=job_queue,
                jobDefinition=job_definition,
                containerOverrides={"environment": [{"value": key, "name": "KEY"}]},
            )
            logging.info("submitting job to compute metadata of {}".format(key))
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "AccessDeniedException":
                logging.error(
                    "ERROR: Access denied to {}. Detail {}".format(job_queue, e)
                )
                sys.exit(1)
            if e.response["Error"]["Code"] != "TooManyRequestsException":
                n_tries += 1
                logging.info("{}. Retry {}".format(e, n_tries))
            else:
                logging.info("TooManyRequestsException. Sleep and retry...")

        time.sleep(2 ** n_tries)
    return False


def submit_jobs(job_queue, job_definition, keys):
    """
    Submit jobs to the queue

    Args:
        job_queue(str): job queue name
        job_definition(str): job definition name
        keys(list(str)): list of object keys

    Returns:
        None
    """
    par_submit_job = partial(submit_job, job_queue, job_definition)
    with Pool(NUMBER_OF_THREADS) as pool:
        pool.map(par_submit_job, keys["Key"])


def list_objects(bucket_name):
    """
    Compute md5 of a bucket object
    """
    # Initialize a storage client
    client = storage.Client()
    # Get client authorized session
    sess = AuthorizedSession(client._credentials)
    result = []
    # If md5 is in the object metadata. Return it and exit
    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)
    for blob in blobs:
        result.append({"Key": blob.name, "Size": blob.size})

        #print(f"{blob.name}, {blob.size}")
    return result

def get_messages_from_queue(queue_url, n_total_messages):
    """
    Args:
        queue_url(str): SQS url
        n_total_messages(int): The expected number of messages being received

    Returns:
        list(dict): list of message in the format of
        {
            "url": "url_example",
            "md5": "md5_example",
            "size": 10
        }
    """
    logging.info("Start consuming queue {}".format(queue_url))
    # Create SQS client
    sqs = boto3.client("sqs", region_name=REGION)

    n_messages = 0
    files = []
    while n_messages < n_total_messages:
        try:
            # receive a message from SQS queue
            response = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=["SentTimestamp"],
                MaxNumberOfMessages=1,
                VisibilityTimeout=0,
                WaitTimeSeconds=0,
            )
            if n_messages % 10 == 0:
                logging.info(
                    "Received {}/{} messages".format(n_messages, n_total_messages)
                )

            for message in response["Messages"]:
                n_messages += 1
                msgBody = json.loads(message["Body"])
                if "ERROR" in msgBody:
                    msgBody["size"] = 0
                    msgBody["md5"] = msgBody["ERROR"]
                    del msgBody["ERROR"]
                files.append(json.loads(message["Body"]))

                # Delete received message from queue
                receipt_handle = message["ReceiptHandle"]
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

        except ClientError as e:
            logging.error(e)
        except KeyError:
            # Queue is empty. Check again later!!!
            logging.info("SQS queue is empty. Taking a sleep ....")
            time.sleep(10)
    return files


def write_messages_to_tsv(queue_url, n_total_messages, bucket_name):
    """
    Consume the sqs and write results to tsv manifest

    Args:
        queue_url(str): SQS url
        n_total_messages(int): The expected number of messages being received
        bucket_name(str): bucket for uploading the manifest to
        authz_file(str): authz data file
    """
    files = get_messages_from_queue(queue_url, n_total_messages)

    authz_objects = {}
    # Default filenames without merging
    fields = ["url", "size", "md5"]

    if len(files) > 0:
        parts = urlparse(files[0]["url"])
        now = datetime.now()
        current_time = now.strftime("%m_%d_%y_%H:%M:%S")

        filename = "manifest_{}_{}.tsv".format(parts.netloc, current_time)
        utils.write_tsv(filename, files, fields)

        utils.upload_file(filename, bucket_name)
        logging.info(
            "Output manifest is stored at s3://{}/{}".format(bucket_name, filename)
        )

    logging.info("DONE!!!")
