import os
import sys

import csv
from functools import partial
from multiprocessing.pool import Pool
import time
import logging

import boto3
from botocore.exceptions import ClientError

from batch_jobs.bin.settings import (
    POSTFIX_1_EXCEPTION,
    POSTFIX_2_EXCEPTION,
    PROJECT_ACL,
    GDC_TOKEN,
)

logging.basicConfig(level=logging.INFO)
# logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

NUMBER_OF_THREADS = 16
MAX_RETRIES = 10

REGION = os.environ.get("REGION", "us-east-1")


def run_job(
    manifest_file,
    job_queue,
    job_definition,
    destination_bucket,
    thread_count=NUMBER_OF_THREADS,
    max_retries=MAX_RETRIES,
):
    """
    Start to run an job to generate bucket manifest
    Args:
        file_info(dict): file info
        job_queue(str): job queue name
        job_definition(str): job definition name

    Returns:
        bool: True if the job was submitted successfully
    """
    global NUMBER_OF_THREADS
    global MAX_RETRIES

    NUMBER_OF_THREADS = thread_count
    MAX_RETRIES = max_retries

    local_manifest = get_manifest_from_bucket(manifest_file)
    parsed_data = parse_manifest_file(local_manifest)
    submitted, skipped, failed = submit_jobs(
        parsed_data, job_queue, job_definition, destination_bucket
    )

    logging.info(f"Job submission summary:")
    logging.info(f"Submitted: {submitted} jobs")
    logging.info(f"Skipped: {skipped} files (already exist)")
    logging.info(f"Failed: {failed} submissions")


def submit_job(job_queue, job_definition, file):
    key = file["id"] + "/" + file["file_name"]

    # Pre-check if file already exists
    exists, message = check_file_exists(
        file["destination_bucket"],
        key,
        int(file["size"]),
        file["md5"],
    )

    if exists:
        logging.info(f"Skipping {key}: {message}")
        return "SKIPPED"

    client = boto3.client("batch", region_name=REGION)
    n_tries = 0
    while n_tries < MAX_RETRIES:
        try:
            client.submit_job(
                jobName="gdc_copy",
                jobQueue=job_queue,
                jobDefinition=job_definition,
                containerOverrides={
                    "environment": [
                        {"value": file["id"], "name": "ID"},
                        {"value": file["file_name"], "name": "FILE_NAME"},
                        {"value": file["size"], "name": "SIZE"},
                        {
                            "value": file["destination_bucket"],
                            "name": "DESTINATION_BUCKET",
                        },
                        {"value": key, "name": "KEY"},
                        {"value": GDC_TOKEN, "name": "GDC_TOKEN"},
                    ]
                },
            )
            logging.info("submitting job to copy file {}".format(key))
            return "SUBMITTED"
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
    return "FAILED"


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

    submitted_count = 0
    skipped_count = 0
    failed_count = 0

    with Pool(NUMBER_OF_THREADS) as pool:
        results = pool.map(par_submit_job, file_info)

    for result in results:
        if result == "SUBMITTED":
            submitted_count += 1
        elif result == "SKIPPED":
            skipped_count += 1
        elif result == "FAILED":
            failed_count += 1

    return submitted_count, skipped_count, failed_count


def parse_manifest_file(manifest_file):
    try:
        with open(manifest_file, mode="r", newline="", encoding="utf-8") as csv_file:
            # Create a DictReader object
            parsed_data = []
            csv_reader = csv.DictReader(csv_file, delimiter="\t")
            # Read the header
            # Iterate through each row in the CSV
            for row in csv_reader:
                fi = {}
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
    Maps a project ID to its corresponding AWS bucket prefix
    """
    if fi["project_id"] in PROJECT_ACL:
        bucket = PROJECT_ACL[fi["project_id"]]["aws_bucket_prefix"]
    else:
        raise ValueError(
            f"Project ID {fi['project_id']} not found in the mapping. Available projects: {list(PROJECT_ACL.keys())}"
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


def get_manifest_from_bucket(s3_location):
    """
    Get the manifest file from the bucket

    Args:
        s3_location(str): s3 location of the manifest file

    Returns:
        str: path to the manifest file
    """
    s3 = boto3.client(
        "s3",
    )

    bucket, key = s3_location.replace("s3://", "").split("/", 1)
    local_manifest = "/tmp/{}".format(key.split("/")[-1])
    try:
        logging.info(f"Attempting to download manifest {key} from s3 bucket {bucket} ")
        s3.download_file(bucket, key, local_manifest)
        logging.info("Retrieved manifest from s3")
    except Exception as e:
        logging.error(f"Error occured: {e}")
        raise (e)
    return local_manifest


def check_file_exists(destination_bucket, key, expected_size, expected_md5=None):
    """
    Check if file already exists in S3 with correct size and MD5
    """
    s3 = boto3.client("s3")

    try:
        response = s3.head_object(Bucket=destination_bucket, Key=key)
        existing_size = response["ContentLength"]
        existing_etag = response["ETag"].strip('"')

        if existing_size == expected_size:
            if expected_md5 and expected_md5 != "None":
                # For single-part uploads, ETag is the MD5
                if existing_etag == expected_md5:
                    return True, "File exists with matching size and MD5"
                else:
                    return (
                        False,
                        f"File exists but MD5 mismatch: {existing_etag} vs {expected_md5}",
                    )
            else:
                return True, "File exists with matching size (MD5 not verified)"
        else:
            return (
                False,
                f"File exists but size mismatch: {existing_size} vs {expected_size}",
            )

    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False, "File does not exist"
        else:
            return False, f"Error checking file: {e}"
