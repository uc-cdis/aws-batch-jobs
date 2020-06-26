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


def run_job(source_bucket, destination_bucket, job_queue, job_definition):
    """
    Start to run an job to generate bucket manifest
    Args:
        source_bucket(str): source bucket name
        destination_bucket(str): destination bucket name
        job_queue(str): job queue name
        job_definition(str): job definition name

    
    Returns:
        bool: True if the job was submitted successfully
    """
    keys = list_objects(source_bucket)
    submit_jobs(source_bucket, destination_bucket, job_queue, job_definition, keys)


def submit_job(source_bucket, destination_bucket, job_queue, job_definition, key):
    """
    Submit job to the job queue

    Args:
        source_bucket(str): source bucket
        destination_bucket(str): destination bucket
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
                jobName="bucket_replicate",
                jobQueue=job_queue,
                jobDefinition=job_definition,
                containerOverrides={"environment": [{"value": key, "name": "KEY"},{"value": source_bucket, "name": "SOURCE_BUCKET"},{"value": destination_bucket, "name": "DESTINATION_BUCKET"}]},
            )
            logging.info("submitting job to copy file {}".format(key))
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


def submit_jobs(source_bucket, destination_bucket, job_queue, job_definition, keys):
    """
    Submit jobs to the queue

    Args:
        source_bucket(str): source bucket
        destination_bucket(str): destination bucket
        job_queue(str): job queue name
        job_definition(str): job definition name
        keys(list(str)): list of object keys
    
    Returns:
        None
    """
    par_submit_job = partial(submit_job, source_bucket, destination_bucket, job_queue, job_definition, keys)
    with Pool(NUMBER_OF_THREADS) as pool:
        pool.map(par_submit_job, keys)


def list_objects(bucket_name):
    """
    List all objects in the bucket

    Args:
        bucket_name(str): the bucket name

    Returns:
        list(str): list of the objects
    """
    result = []

    with open("/bucket-replicate/creds.json") as creds_file:
        creds = json.load(creds_file)
        aws_access_key_id = creds.get("aws_access_key_id")
        aws_secret_access_key = creds.get("aws_secret_access_key")

    client = boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    try:
        paginator = client.get_paginator("list_objects_v2")
        logging.info("start to list objects in {}".format(bucket_name))
        pages = paginator.paginate(Bucket=bucket_name, RequestPayer="requester")
        for page in pages:
            for obj in page["Contents"]:
                result.append(obj["Key"])
    except ClientError as e:
        logging.error(
            "Can not list objects in the bucket {}. Detail {}".format(bucket_name, e)
        )

    return result

