import csv
from functools import partial
import io
import logging
from multiprocessing.pool import Pool

import sys
import os
import math
import time

import boto3
import botocore
from botocore.exceptions import ClientError
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


### AWS Batch globals
NUMBER_OF_THREADS = 16
MAX_RETRIES = 10
REGION = os.environ.get("REGION", "us-east-1")


### Replication Job globals
manifest_file = "/Users/binambajracharya/Downloads/GDC_only_3plusgig_open_manifest.tsv"
data_endpoint = "https://api.gdc.cancer.gov/data/"
DATA_ENDPT = data_endpoint
RETRIES_NUM = 4
GDC_TOKEN = ""
TARGET_BUCKET = "gdc-archive-prod-open-2"
download_chunk_size = 1024 * 1024 * 100  # 100 MB
UPLOAD_CHUNK_SIZE = 1024 * 1024 * 100  # 100 MB

retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
)
download_adapter = HTTPAdapter(max_retries=retry_strategy)


def run_job(manifest_file, job_queue, job_definition):
    """
    Start to run a job to copy files from GDC to S3
    Args:
        manifest_file(str): manifest file name
        job_queue(str): job queue name
        job_definition(str): job definition name

    Returns:
        bool: True if the job was submitted successfully
    """
    pass


def generate_chunk_data_list(size, data_size):
    L = []
    idx = 0
    size = int(size)
    data_size = int(data_size)
    while idx < size:
        L.append((idx, min(idx + data_size - 1, size - 1)))
        idx += data_size

    return L


def calculate_total_parts(file_size_bytes, chunk_size_bytes):
    """
    Calculate total parts for predetermined chunk size

    Args:
        file_size_bytes (int): Total file size in bytes
        chunk_size_bytes (int): Fixed chunk size in bytes

    Returns:
        int: Total number of parts required

    Raises:
        ValueError: If chunk size violates AWS constraints
    """

    # AWS S3 requirements
    MIN_CHUNK = 5 * 1024**2  # 5 MiB
    MAX_PARTS = 10_000
    file_size_bytes = int(file_size_bytes)
    chunk_size_bytes = int(chunk_size_bytes)

    # Validate chunk size meets AWS minimum
    if chunk_size_bytes < MIN_CHUNK:
        raise ValueError(
            f"Chunk size {chunk_size_bytes} bytes too small. "
            f"Minimum 5 MiB ({MIN_CHUNK} bytes) required"
        )

    # Calculate total parts
    total_parts = math.ceil(file_size_bytes / chunk_size_bytes)

    # Verify AWS part limit
    if total_parts > MAX_PARTS:
        max_allowed_size = MAX_PARTS * chunk_size_bytes
        raise ValueError(
            f"File too large ({file_size_bytes} bytes) for {chunk_size_bytes} byte chunks. "
            f"Max allowed: {max_allowed_size} bytes with {MAX_PARTS} parts"
        )

    return total_parts


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


def stream_object_from_gdc_api(fi, target_bucket=None):
    """
    Stream an object from the GDC API to a target bucket.
    """

    def _handler_multipart(chunk_info):
        """
        Handler for multipart upload.
        """
        part_number = chunk_info["part_number"]
        for attempt in range(RETRIES_NUM + 1):

            try:
                session = requests.Session()
                session.mount("https://", download_adapter)

                headers = {
                    "X-Auth-Token": GDC_TOKEN,
                    "Range": f"bytes={chunk_info['start']}-{chunk_info['end']}",
                }

                print(f"Downloading {chunk_info['start']} to {chunk_info['end']}")
                with session.get(
                    data_endpoint,
                    headers=headers,
                    stream=True,
                ) as response:
                    response.raise_for_status()
                    expected_size = chunk_info["end"] - chunk_info["start"] + 1
                    downloaded_data = io.BytesIO()

                    print(f"Downloaded {expected_size} bytes for part {part_number}")

                    # Stream download in 8KB chunks
                    for chunk in response.iter_content(chunk_size=download_chunk_size):
                        if not chunk:
                            continue
                        downloaded_data.write(chunk)

                    # Validate downloaded size
                    actual_size = downloaded_data.tell()

                    # Upload full S3 part chunk
                    downloaded_data.seek(0)
                    res = thread_s3.upload_part(
                        Body=downloaded_data,
                        Bucket=target_bucket,
                        Key=object_path,
                        PartNumber=part_number,
                        UploadId=multipart_upload["UploadId"],
                    )

                    return res, part_number, expected_size
            except Exception as e:
                print(
                    f"Download failed for {data_endpoint} with error: {e} after {attempt} attempts"
                )
            time.sleep(min(2**attempt, 10))

    thread_s3 = boto3.client("s3")
    object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))
    data_endpoint = (
        DATA_ENDPT + fi.get("id", "")
        if DATA_ENDPT
        else "https://api.gdc.cancer.gov/data/{}".format(fi.get("id"))
    )
    size = int(fi.get("size"))

    try:
        multipart_upload = thread_s3.create_multipart_upload(
            Bucket=target_bucket,
            Key=object_path,
            ACL="bucket-owner-full-control",
        )
    except botocore.exceptions.ClientError as error:
        print(
            "Error when create multiple part upload for object with uuid {}. Detail {}".format(
                object_path, error
            )
        )
        return

    chunk_data_size = UPLOAD_CHUNK_SIZE

    tasks = []
    for part_number, data_range in enumerate(
        generate_chunk_data_list(fi["size"], chunk_data_size)
    ):
        start, end = data_range
        tasks.append({"start": start, "end": end, "part_number": part_number + 1})

    total_parts = calculate_total_parts(fi["size"], chunk_data_size)

    results = {}
    for task in tasks:
        try:
            results = _handler_multipart(task)
            print(f"Uploaded {len(results)} parts for file {fi['file_name']}")
        except Exception as e:
            print(f"Fatal error in transfer: {e}")

    parts = []
    total_bytes_received = 0

    sorted_results = sorted(results, key=lambda x: x[1])

    for res, part_number, chunk_size in sorted_results:
        parts.append({"ETag": res["ETag"], "PartNumber": part_number})
        total_bytes_received += chunk_size

    try:
        print(f"Starting multipart upload for {object_path}")
        thread_s3.complete_multipart_upload(
            Bucket=target_bucket,
            Key=object_path,
            MultipartUpload={"Parts": parts},
            UploadId=multipart_upload["UploadId"],
            RequestPayer="requester",
        )
        print(f"Successfully Completed Multipart Upload for {object_path}")
    except botocore.exceptions.ClientError as error:
        print(
            "Error when finishing multiple part upload object with uuid {}. Detail {}".format(
                fi.get("id"), error
            )
        )
        return


def exec_aws_copy(manifest_file):
    parsed_data = parse_manifest_file(manifest_file)
    for fi in parsed_data:
        # each fi will spawn a new container.
        # each container will run the stream_object_from_gdc_api function
        stream_object_from_gdc_api(fi, target_bucket=TARGET_BUCKET)
        print(f"Successfully uploaded {fi['file_name']} to {TARGET_BUCKET}/{fi['id']}")


exec_aws_copy(manifest_file)
