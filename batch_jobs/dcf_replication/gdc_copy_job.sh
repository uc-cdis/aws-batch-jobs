#!/bin/bash
set -uxo pipefail

aws configure set aws_access_key_id "$ACCESS_KEY_ID"
aws configure set aws_secret_access_key "$SECRET_ACCESS_KEY"
echo "aws credentials configured."

if [[ "$DESTINATION_BUCKET" == s3://* ]]; then
    S3_PREFIX="${DESTINATION_BUCKET%/}"
else
    S3_PREFIX="s3://$DESTINATION_BUCKET"
fi

S3_OBJ="$S3_PREFIX/$KEY"
MAX_RETRIES=3
RETRY_DELAY=10
attempt=1
success=false

while [ "$attempt" -le "$MAX_RETRIES" ]; do
    if python3 - <<EOF
import urllib
import boto3
import hashlib
import sys
import time
import math

DATA_ENDPOINT = "https://api.gdc.cancer.gov/data/${ID}"
GDC_TOKEN     = "${GDC_TOKEN}"
TARGET_BUCKET = "${DESTINATION_BUCKET}"
OBJECT_PATH   = "${KEY}"
FILE_SIZE     = int("${SIZE}")
EXPECTED_MD5  = "${MD5SUM:-}"
CHUNK_SIZE    = 128 * 1024 * 1024
PIECE_SIZE    = 64 * 1024 * 1024
RETRIES_NUM   = 3

def generate_chunk_data_list(size, data_size):
    L = []
    idx = 0
    while idx < size:
        L.append((idx, min(idx + data_size - 1, size - 1)))
        idx += data_size

    return L

s3 = boto3.client("s3")

multipart = s3.create_multipart_upload(Bucket=TARGET_BUCKET, Key=OBJECT_PATH, ACL="bucket-owner-full-control")
upload_id = multipart["UploadId"]
print(f"Started multipart upload: {upload_id}")

parts       = []
md5_hash    = hashlib.md5()
total_parts = math.ceil(FILE_SIZE / CHUNK_SIZE)
uploaded    = 0

try:
    for n_part, data_range in enumerate(generate_chunk_data_list(FILE_SIZE, CHUNK_SIZE)):
        start, end = data_range
        part_number = n_part + 1
        # Download chunk with retries
        chunk            = None
        tries            = 0
        chunk_downloaded = False

        while tries < RETRIES_NUM and not chunk_downloaded:
            try:
                response = urllib.request.Request(
                    DATA_ENDPOINT,
                    headers={
                        "X-Auth-Token": GDC_TOKEN,
                        "Range": f"bytes={start}-{end}",
                    },
                )

                chunk = urllib.request.urlopen(response).read()

                if len(chunk) == end - start + 1:
                    chunk_downloaded = True
                else:
                    print(f"Chunk size mismatch: expected {end - start + 1}, got {len(chunk)}")
                    chunk = None
                    tries += 1
                    time.sleep(5)
                print(f"Downloading {DATA_ENDPOINT}: {part_number(end - start + 1)}/{FILE_SIZE}")

            except Exception as e:
                print(f"Error downloading part {part_number} (attempt {tries + 1}): {e}")
                chunk = None
                tries += 1
                time.sleep(5)

        if not chunk_downloaded:
            raise Exception(f"Failed to download part {part_number} after {RETRIES_NUM} retries")

        # Upload part with retries
        tries         = 0
        part_uploaded = False

        while tries < RETRIES_NUM and not part_uploaded:
            try:
                res = s3.upload_part(
                    Body=chunk,
                    Bucket=TARGET_BUCKET,
                    Key=OBJECT_PATH,
                    PartNumber=part_number,
                    UploadId=upload_id,
                )
                parts.append({"PartNumber": part_number, "ETag": res["ETag"]})
                part_uploaded = True
            except Exception as e:
                print(f"Error uploading part {part_number} (attempt {tries + 1}): {e}")
                tries += 1
                time.sleep(5)

        if not part_uploaded:
            raise Exception(f"Failed to upload part {part_number} after {RETRIES_NUM} retries")

        # Update overall md5 and progress, then free memory
        md5_hash.update(chunk)
        uploaded += len(chunk)
        chunk = None
        print(f"Part {part_number}/{total_parts} done ({uploaded / 1024 / 1024:.1f} MB uploaded)")

    # Complete multipart upload
    s3.complete_multipart_upload(
        Bucket=TARGET_BUCKET,
        Key=OBJECT_PATH,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    print("Multipart upload complete.")

    # Validate size
    if FILE_SIZE and uploaded != FILE_SIZE:
        print(f"Size mismatch: expected {FILE_SIZE}, got {uploaded}")
        s3.delete_object(Bucket=TARGET_BUCKET, Key=OBJECT_PATH)
        sys.exit(1)
    else:
        print(f"Size validation passed: {uploaded} bytes")

    # Validate md5
    final_md5 = md5_hash.hexdigest()
    print(f"Computed MD5: {final_md5}")
    if EXPECTED_MD5 and final_md5 != EXPECTED_MD5:
        print(f"MD5 mismatch: expected {EXPECTED_MD5}, got {final_md5}")
        s3.delete_object(Bucket=TARGET_BUCKET, Key=OBJECT_PATH)
        sys.exit(1)
    elif EXPECTED_MD5:
        print("MD5 validation passed.")
    else:
        print("MD5SUM not set, skipping validation.")

    sys.exit(0)

except Exception as e:
    print(f"ERROR: {e}")
    try:
        s3.abort_multipart_upload(
            Bucket=TARGET_BUCKET,
            Key=OBJECT_PATH,
            UploadId=upload_id,
        )
        print("Multipart upload aborted.")
    except Exception as abort_err:
        print(f"Failed to abort multipart upload: {abort_err}")
    sys.exit(1)
EOF
    then
        echo "Download validation passed, upload complete."
        success=true
        break
    else
        echo "Transfer failed on attempt $attempt"
    fi

    echo "Attempt $attempt failed, sleeping $RETRY_DELAY seconds then retrying..."
    sleep "$RETRY_DELAY"
    attempt=$((attempt + 1))
done

if [ "$success" = false ]; then
    echo "ERROR: File transfer failed after $MAX_RETRIES attempts"
    exit 1
fi

echo "SUCCESS: File verified and transferred"
exit 0
