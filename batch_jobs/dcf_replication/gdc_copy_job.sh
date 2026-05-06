#!/bin/bash
set -uxo pipefail

ls -R

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

DATA_ENDPOINT = "https://api.gdc.cancer.gov/data/${ID}"
GDC_TOKEN     = "${GDC_TOKEN}"
TARGET_BUCKET = "${DESTINATION_BUCKET}"
OBJECT_PATH   = "${KEY}"
FILE_SIZE     = int("${SIZE}")
EXPECTED_MD5  = "${MD5SUM:-}"
CHUNK_SIZE    = 128 * 1024 * 1024


command="python3 ./batch_jobs/dcf_replication/file_get_upload.py --file-upload $ID --gdc_token $GDC_TOKEN --target_bucket $DESTINATION_BUCKET --object-path $OBJECT_PATH --file_size $FILE_SIZE --expected_md5 $EXPECTED_MD5"

while [ "$attempt" -le "$MAX_RETRIES" ]; do
    if $command; then
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
