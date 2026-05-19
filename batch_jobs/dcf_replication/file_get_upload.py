import argparse
import hashlib
import math
import sys
import time
import urllib

import boto3

RETRIES_NUM = 3


def generate_chunk_data_list(size, chunk_size):
    L = []
    idx = 0
    while idx < size:
        L.append((idx, min(idx + chunk_size - 1, size - 1)))
        idx += chunk_size

    return L


def api_to_bucket_copy(
    file_id,
    gdc_token,
    target_bucket,
    object_path,
    file_size,
    expected_md5,
    chunk_size,
    retries_num,
):

    DATA_ENDPOINT = f"https://api.gdc.cancer.gov/data/{file_id}"

    s3 = boto3.client("s3")

    multipart = s3.create_multipart_upload(
        Bucket=target_bucket, Key=object_path, ACL="bucket-owner-full-control"
    )
    upload_id = multipart["UploadId"]
    print(f"Started multipart upload: {upload_id}")

    parts = []
    md5_hash = hashlib.md5()
    total_parts = math.ceil(file_size / chunk_size)
    uploaded = 0

    try:
        for n_part, data_range in enumerate(
            generate_chunk_data_list(file_size, chunk_size)
        ):
            start, end = data_range
            part_number = n_part + 1
            # Download chunk with retries
            chunk = None
            download_tries = 0
            chunk_downloaded = False

            while download_tries < retries_num and not chunk_downloaded:
                try:
                    response = urllib.request.Request(
                        DATA_ENDPOINT,
                        headers={
                            "X-Auth-Token": gdc_token,
                            "Range": f"bytes={start}-{end}",
                        },
                    )

                    chunk = urllib.request.urlopen(response).read()

                    if len(chunk) == end - start + 1:
                        chunk_downloaded = True
                    else:
                        print(
                            f"Chunk size mismatch: expected {end - start + 1}, got {len(chunk)}"
                        )
                        chunk = None
                        download_tries += 1
                        time.sleep(5)
                    print(
                        f"Downloading {DATA_ENDPOINT}: {part_number*(end - start + 1)}/{file_size}"
                    )

                except Exception as e:
                    print(
                        f"Error downloading part {part_number} (attempt {download_tries + 1}): {e}"
                    )
                    chunk = None
                    download_tries += 1
                    time.sleep(5)

            if not chunk_downloaded:
                raise Exception(
                    f"Failed to download part {part_number} after {retries_num} retries"
                )

            # Upload part with retries
            upload_tries = 0
            part_uploaded = False

            while upload_tries < retries_num and not part_uploaded:
                try:
                    res = s3.upload_part(
                        Body=chunk,
                        Bucket=target_bucket,
                        Key=object_path,
                        PartNumber=part_number,
                        UploadId=upload_id,
                    )
                    parts.append({"PartNumber": part_number, "ETag": res["ETag"]})
                    part_uploaded = True
                except Exception as e:
                    print(
                        f"Error uploading part {part_number} (attempt {upload_tries + 1}): {e}"
                    )
                    upload_tries += 1
                    time.sleep(5)

            if not part_uploaded:
                raise Exception(
                    f"Failed to upload part {part_number} after {retries_num} retries"
                )

            # Update overall md5 and progress, then free memory
            md5_hash.update(chunk)
            uploaded += len(chunk)
            chunk = None
            print(
                f"Part {part_number}/{total_parts} done ({uploaded / 1024 / 1024:.1f} MB uploaded)"
            )

        # Complete multipart upload
        s3.complete_multipart_upload(
            Bucket=target_bucket,
            Key=object_path,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        print("Multipart upload complete.")

        # Validate size
        if file_size and uploaded != file_size:
            print(f"Size mismatch: expected {file_size}, got {uploaded}")
            s3.delete_object(Bucket=target_bucket, Key=object_path)
            sys.exit(1)
        else:
            print(f"Size validation passed: {uploaded} bytes")

        # Validate md5
        final_md5 = md5_hash.hexdigest()
        print(f"Computed MD5: {final_md5}")
        if expected_md5 and final_md5 != expected_md5:
            print(f"MD5 mismatch: expected {expected_md5}, got {final_md5}")
            s3.delete_object(Bucket=target_bucket, Key=object_path)
            sys.exit(1)
        elif expected_md5:
            print("MD5 validation passed.")
        else:
            print("MD5SUM not set, skipping validation.")

        sys.exit(0)

    except Exception as e:
        print(f"ERROR: {e}")
        try:
            s3.abort_multipart_upload(
                Bucket=target_bucket,
                Key=object_path,
                UploadId=upload_id,
            )
            print("Multipart upload aborted.")
        except Exception as abort_err:
            print(f"Failed to abort multipart upload: {abort_err}")
        sys.exit(1)


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(title="action", dest="action")

    file_get_upload_cmd = subparser.add_parser("upload_data")
    file_get_upload_cmd.add_argument(
        "--file_id",
        required=True,
        help="uuid for the file to be copied",
    )
    file_get_upload_cmd.add_argument(
        "--gdc_token",
        required=True,
        help="Token from GDC to use in the GDC download API",
    )
    file_get_upload_cmd.add_argument(
        "--target_bucket",
        required=True,
        help="S3 Bucket for the files to be uploaded to",
    )
    file_get_upload_cmd.add_argument(
        "--object_path",
        required=True,
        help="Path at which the object will be uploaded to in the destination bucket",
    )
    file_get_upload_cmd.add_argument(
        "--file_size",
        required=True,
        help="File size as defined in the GDC manifest. This will be used to validate uplaod",
    )
    file_get_upload_cmd.add_argument(
        "--expected_md5",
        required=True,
        help="MD5 as given in the GDC manifest. This will be used to validate upload",
    )
    file_get_upload_cmd.add_argument(
        "--chunk_size",
        required=False,
        default=128 * 1024 * 1024,
        help="Size of chunk to be downloaded from the GDC API and the size of chunk that is uploaded to the S3 bucket in multi-part upload",
    )
    file_get_upload_cmd.add_argument(
        "--retry",
        required=False,
        default=3,
        help="Number of retries for both download and upload",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    if args.action == "upload_data":
        api_to_bucket_copy(
            args.file_id,
            args.gdc_token,
            args.target_bucket,
            args.object_path,
            int(args.file_size),
            args.expected_md5,
            int(args.chunk_size),
            int(args.retry),
        )
