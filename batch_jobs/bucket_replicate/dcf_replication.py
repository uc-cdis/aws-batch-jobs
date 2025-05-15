import os
import sys

import csv
from functools import partial
from multiprocessing.pool import Pool
import time
import logging

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
# logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

NUMBER_OF_THREADS = 16
MAX_RETRIES = 10

REGION = os.environ.get("REGION", "us-east-1")
parsed_data = [
    {
        "id": "3dc0e394-dd06-4e7d-b85b-dac258c1ed95",
        "file_name": "TCGA-YL-A8SA-01Z-00-DX1.B246D547-9875-42DB-8BD9-206C449EF089.svs",
        "size": "3007580564",
        "destination_bucket": "bucket-1",
    },
    {
        "id": "3dc0e394-dd06-4e7d-b85b-dac258c1ed95",
        "file_name": "TCGA-YL-A8SA-01Z-00-DX1.B246D547-9875-42DB-8BD9-206C449EF089.svs",
        "size": "3007580564",
        "destination_bucket": "bucket-2",
    },
    {
        "id": "3dc0e394-dd06-4e7d-b85b-dac258c1ed95",
        "file_name": "TCGA-YL-A8SA-01Z-00-DX1.B246D547-9875-42DB-8BD9-206C449EF089.svs",
        "size": "3007580564",
        "destination_bucket": "bucket-3",
    },
]


def run_job(parsed_data, job_queue, job_definition):
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

    pass


def submit_job(file_info, job_queue, job_definition):

    client = boto3.client("batch", region_name=REGION)
    n_tries = 0
    while n_tries < MAX_RETRIES:
        try:
            key = file_info["id"] + "/" + file_info["file_name"]
            client.submit_job(
                jobName="object_copy",
                jobQueue=job_queue,
                jobDefinition=job_definition,
                containerOverrides={
                    "environment": [
                        {"value": file_info["id"], "name": "ID"},
                        {"value": file_info["file_name"], "name": "FILE_NAME"},
                        {"value": file_info["size"], "name": "SIZE"},
                        {
                            "value": file_info["destination_bucket"],
                            "name": "DESTINATION_BUCKET",
                        },
                        {"value": key, "name": "KEY"},
                    ]
                },
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

        time.sleep(2**n_tries)
    return False


def submit_jobs(file_info, job_queue, job_definition):
    """
    Submit jobs to the queue

    Args:
        file_info(dict): file info
        job_queue(str): job queue name
        job_definition(str): job definition name

    Returns:
        bool: True if the job was submitted successfully
    """

    par_submit_job = partial(submit_job(file_info, job_queue, job_definition))
    with Pool(NUMBER_OF_THREADS) as pool:
        pool.map(par_submit_job, file_info)


# Not using at the moment but will use later to parase a GDC manifest
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
            for row in csv_reader:
                fi["id"] = row["id"]
                fi["file_name"] = row["file_name"]
                fi["size"] = row["size"]
                i += 1
                if i == 4:
                    break
                parsed_data.append(fi)
        return parsed_data
    except Exception as e:
        print(f"An error occurred: {str(e)}")
