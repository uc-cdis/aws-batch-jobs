import os
import sys

import csv
from functools import partial
from multiprocessing.pool import Pool
import time
import logging
import io
import datetime

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

JOB_STATUS_KEY = "job_status"
REGION = os.environ.get("REGION", "us-east-1")
NUMBER_OF_THREADS = 5
MAX_RETRIES = 3


def run_job(
    manifest_file,
    job_queue,
    job_definition,
    output_manifest_bucket,
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

    NUMBER_OF_THREADS = int(thread_count)
    MAX_RETRIES = int(max_retries)
    START_TIME = int(time.time())
    logging.info(
        f"Submission Job started at {START_TIME} with {NUMBER_OF_THREADS} threads and {MAX_RETRIES} retries."
    )
    local_manifest = get_manifest_from_bucket(manifest_file)
    parsed_data = parse_manifest_file(local_manifest)
    submitted, skipped, failed = submit_jobs(
        parsed_data, job_queue, job_definition, output_manifest_bucket
    )

    logging.info(f"Job submission summary:")
    logging.info(f"Submitted: {submitted} jobs")
    logging.info(f"Skipped: {skipped} files (already exist)")
    logging.info(f"Failed: {failed} submissions")
    logging.info(f"Job starting time is {START_TIME}")
    logging.info(f"Job ending time is {int(time.time())}")


def submit_job(job_queue, job_definition, file):
    key = file["id"] + "/" + file["file_name"]
    session = boto3.Session()
    s3 = session.client("s3")

    # Pre-check if bucket exists
    if not check_bucket_exists(s3, file["destination_bucket"]):
        logging.error(
            "Destination bucket does not exist in s3: {}".format(
                file["destination_bucket"]
            )
        )
        file[JOB_STATUS_KEY] = "FAILED"
        return file

    # Pre-check if file already exists
    exists, message = check_file_exists(
        s3,
        file["destination_bucket"],
        key,
        int(file["size"]),
        file["md5"],
    )

    if exists:
        logging.info(f"Skipping {key}: {message}")
        file[JOB_STATUS_KEY] = "SKIPPED"
        return file

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
                        {"value": file["md5"], "name": "MD5SUM"},
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
            file[JOB_STATUS_KEY] = "SUBMITTED"
            return file
        except ClientError as e:
            if e.response["Error"]["Code"] == "AccessDeniedException":
                logging.error(
                    "ERROR: Access denied to {}. Detail {}".format(job_queue, e)
                )
                sys.exit(1)
            if e.response["Error"]["Code"] == "TooManyRequestsException":
                logging.info("TooManyRequestsException. Sleep and retry...")
            else:
                n_tries += 1
                logging.info("{}. Retry {}".format(e, n_tries))

        time.sleep(2**n_tries)
    file[JOB_STATUS_KEY] = "FAILED"
    return file


def submit_jobs(file_info, job_queue, job_definition, output_manifest_bucket):
    """
    Submit jobs to the queue

    Args:
        file_info(dict): file info
        job_queue(str): job queue name
        job_definition(str): job definition name

    Returns:
        bool: True if the job was submitted successfully
    """

    par_submit_job = partial(submit_job, job_queue, job_definition)

    submitted_count = 0
    skipped_count = 0
    failed_count = 0
    submitted_output_manifest = []
    skipped_output_manifest = []
    failed_output_manifest = []

    with Pool(NUMBER_OF_THREADS) as pool:
        results = pool.map(par_submit_job, file_info)

    for result in results:
        if result[JOB_STATUS_KEY] == "SUBMITTED":
            submitted_count += 1
            submitted_output_manifest.append(
                convert_file_info_to_output_manifest(result)
            )
        elif result[JOB_STATUS_KEY] == "SKIPPED":
            skipped_count += 1
            skipped_output_manifest.append(convert_file_info_to_output_manifest(result))
        elif result[JOB_STATUS_KEY] == "FAILED":
            failed_count += 1
            failed_output_manifest.append(convert_file_info_to_output_manifest(result))

    write_output_manifest_to_s3_file(
        submitted_output_manifest, output_manifest_bucket, "dcf_aws_batch_submitted"
    )
    write_output_manifest_to_s3_file(
        skipped_output_manifest, output_manifest_bucket, "dcf_aws_batch_skipped"
    )
    write_output_manifest_to_s3_file(
        failed_output_manifest, output_manifest_bucket, "dcf_aws_batch_failed"
    )

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
                fi["acl"] = (
                    row["acl"].replace("u'", "'").strip()
                )  # to normalize the acl
                fi["md5"] = row["md5"]
                fi["baseid"] = row["baseid"]
                fi["url"] = row["url"]
                fi["project_id"] = row["project_id"]
                fi["destination_bucket"] = map_project_to_bucket(fi)
                parsed_data.append(fi)
            return parsed_data
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


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
        else:
            if bucket not in POSTFIX_1_EXCEPTION and bucket not in POSTFIX_2_EXCEPTION:
                bucket += "-2-open"
            elif bucket in POSTFIX_1_EXCEPTION:
                bucket += "-open"
            elif bucket in POSTFIX_2_EXCEPTION:
                bucket += "-2-open"
    elif fi["acl"] != "['open']":
        if "target" in bucket:
            bucket = "target-controlled"
        else:
            if bucket not in POSTFIX_1_EXCEPTION and bucket not in POSTFIX_2_EXCEPTION:
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
    session = boto3.Session()
    s3 = session.client("s3")

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


def check_file_exists(s3, destination_bucket, key, expected_size, expected_md5=None):
    """
    Check if file already exists in S3 with correct size and MD5
    """

    try:
        response = s3.head_object(Bucket=destination_bucket, Key=key)
        existing_size = response["ContentLength"]

        if existing_size == expected_size:
            return True, f"File {key} exists with matching size"
        else:
            return (
                False,
                f"File {key} exists but size mismatch: {existing_size} vs {expected_size}",
            )

    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False, "File {key} does not exist"
        else:
            return False, f"Error checking file {key}: {e}"


def check_bucket_exists(s3, bucket_name):
    """
    Check to see if destination bucket exists
    """
    try:
        # Use the s3 client that was passed to the function
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            logging.info(f"Bucket {bucket_name} does not exist")
        elif error_code == "403":
            logging.error(f"Access denied to bucket {bucket_name}")
        else:
            logging.error(f"Error checking bucket {bucket_name}: {e}")
        return False


def convert_file_info_to_output_manifest(file_info):
    """
    Convert file_info to output manifest row for indexing
    Columns: ['guid','md5','size','authz','acl','file_name','urls']
    """
    acls = []
    authz = []
    # Process acl and set authz value
    if file_info.get("acl") == "['open']":
        acls = ["*"]
        authz = ["/open"]
    else:
        acls = [
            acl.strip().replace("'", "")
            for acl in file_info.get("acl", "").strip()[1:-1].split(",")
        ]
        for acl in acls:
            if not acl.startswith("phs"):
                raise Exception(
                    'Only "open" and "phs[...]" ACLs are allowed. Got ACL "{}"'.format(
                        acl
                    )
                )
        authz = ["/programs/{}".format(acl) for acl in acls]

    # Determine final urls
    object_key = "{}/{}".format(file_info.get("id"), file_info.get("file_name"))
    upload_url = "s3://{}/{}".format(file_info["destination_bucket"], object_key)
    urls = [
        "https://api.gdc.cancer.gov/data/{}".format(file_info.get("id", "")),
        upload_url,
    ]

    return {
        "guid": file_info.get("id"),
        "md5": file_info.get("md5"),
        "size": file_info.get("size"),
        "authz": authz,
        "acl": acls,
        "file_name": file_info.get("file_name"),
        "urls": urls,
    }


def write_output_manifest_to_s3_file(data, bucket_name, file_prefix):
    """
    Write output manifest data to tsv file to s3 location
    """
    try:
        time_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
        key = f"{file_prefix}_{time_str}.tsv"
        session = boto3.Session()
        s3 = session.client("s3")
        # Use the s3 client that was passed to the function
        if check_bucket_exists(s3, bucket_name):
            # Create an in-memory text buffer
            csv_buffer = io.StringIO()

            writer = csv.DictWriter(
                csv_buffer,
                fieldnames=["guid", "md5", "size", "authz", "acl", "file_name", "urls"],
                delimiter="\t",
            )
            writer.writeheader()
            writer.writerows(data)

            s3.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=csv_buffer.getvalue(),
                ContentType="text/csv",
            )
            logging.info(
                f"Output Manifest File '{key}' successfully uploaded to S3 bucket '{bucket_name}'."
            )
    except ClientError as e:
        logging.error(f"Error writing output manifest to {bucket_name}: {e}")
        raise (e)
