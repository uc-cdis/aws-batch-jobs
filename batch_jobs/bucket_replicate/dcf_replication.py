import os
import sys

import csv
from functools import partial
from multiprocessing.pool import Pool
import time
import logging

import boto3
from botocore.exceptions import ClientError

# TODO: Create a settings file or figure out a way to directly import these configurations from mounted volume
from settings import POSTFIX_1_EXCEPTION, POSTFIX_2_EXCEPTION

logging.basicConfig(level=logging.INFO)
# logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

NUMBER_OF_THREADS = 16
# TODO: I think we'll have to clean up the retry logic. There's two retries happening.
# One in the submit_job function and another one in attempting to download the file in the object_copy_job.sh script.
# Lets make it clear and have it as separate parameters with defaults values.
MAX_RETRIES = 10

REGION = os.environ.get("REGION", "us-east-1")


def run_job(manifest_file, destination_bucket, job_queue, job_definition):
    """
    Start to run an job to generate bucket manifest
    Args:
        file_info(dict): file info
        destination_bucket(str): destination bucket name
        job_queue(str): job queue name
        job_definition(str): job definition name


    Returns:
        bool: True if the job was submitted successfully
    """
    parsed_data = parse_manifest_file(manifest_file)
    submit_jobs(parsed_data, job_queue, job_definition, destination_bucket)


def submit_job(job_queue, job_definition, destination_bucket, file):

    client = boto3.client("batch", region_name=REGION)
    n_tries = 0
    while n_tries < MAX_RETRIES:
        try:
            key = file["id"] + "/" + file["file_name"]
            # TODO: I think in the POC, we just hardcoded the ACCESS_TOKEN. We should be able to pass it as an environment variable

            client.submit_job(
                jobName="object_copy",
                jobQueue=job_queue,
                jobDefinition=job_definition,
                containerOverrides={
                    "environment": [
                        {"value": file["id"], "name": "ID"},
                        {"value": file["file_name"], "name": "FILE_NAME"},
                        {"value": file["size"], "name": "SIZE"},
                        {
                            "value": destination_bucket,
                            "name": "DESTINATION_BUCKET",
                        },
                        {"value": key, "name": "KEY"},
                    ]
                },
            )
            # TODO: We should be able to figure out whether a job copied a file successfully or not.
            # If not, we should create a failed manfiest that we can resubmit later
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

        time.sleep(2**n_tries)
    return False


def submit_jobs(file_info, job_queue, job_definition, destination_bucket):
    """
    Submit jobs to the queue

    Args:
        file_info(dict): file info
        job_queue(str): job queue name
        job_definition(str): job definition name

    Returns:
        bool: True if the job was submitted successfully
    """

    par_submit_job = partial(submit_job, job_queue, job_definition, destination_bucket)
    with Pool(NUMBER_OF_THREADS) as pool:
        pool.map(par_submit_job, file_info)


def parse_manifest_file(manifest_file):
    try:
        with open(manifest_file, mode="r", newline="", encoding="utf-8") as csv_file:
            # Create a DictReader object
            parsed_data = []
            csv_reader = csv.DictReader(csv_file, delimiter="\t")
            fi = {}
            # Read the header
            # Iterate through each row in the CSV
            i = 0
            list_reader = csv_reader
            for row in csv_reader:
                fi["id"] = row["id"]
                fi["file_name"] = row["file_name"]
                fi["size"] = row["size"]
                fi["acl"] = row["acl"]
                fi["md5"] = row["md5"]
                fi["baseid"] = row["baseid"]
                fi["url"] = row["url"]
                fi["project_id"] = row["project_id"]
                fi["destination_bucket"] = map_project_to_bucket(fi)
                parsed_data.append(fi)
            return parsed_data
    except Exception as e:
        print(f"An error occurred: {str(e)}")


def map_project_to_bucket(fi):
    """
    Maps a project ID to its corresponding AWS and GCS bucket prefixes.
    """

    if fi["project_id"] in PROJECT_MAP:
        bucket = PROJECT_MAP[fi["project_id"]]["aws_bucket_prefix"]
    else:
        raise ValueError(
            f"Project ID {fi['project_id']} not found in the mapping. Available projects: {list(PROJECT_MAP.keys())}"
        )

    if fi["acl"] == "['open']":
        if "target" in bucket:
            bucket = "gdc-target-phs000218-2-open"
        elif bucket not in POSTFIX_1_EXCEPTION and bucket not in POSTFIX_2_EXCEPTION:
            bucket += "-2-open"
        elif bucket in POSTFIX_1_EXCEPTION:
            bucket += "-open"
        elif bucket in POSTFIX_2_EXCEPTION:
            bucket += "-2-open"
        else:
            raise ValueError(
                f"Bucket {fi['bucket']} not recognized. Expected 'open' or 'controlled'."
            )
    elif fi["acl"] != "['open']":
        if "target" in bucket:
            bucket = "target-controlled"
        elif bucket not in POSTFIX_1_EXCEPTION and bucket not in POSTFIX_2_EXCEPTION:
            bucket += "-controlled"
        elif bucket in POSTFIX_1_EXCEPTION:
            bucket += "-controlled"
        elif bucket in POSTFIX_2_EXCEPTION:
            bucket += "-2-controlled"
        else:
            raise ValueError(
                f"Bucket {fi['bucket']} not recognized. Expected 'open' or 'controlled'."
            )
    return bucket
