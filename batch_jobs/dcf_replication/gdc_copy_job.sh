#!/bin/bash
echo "Starting the job.."
set -uxo pipefail  # (same as before; note: no -e here, like your original)

aws configure set aws_access_key_id "$ACCESS_KEY_ID"
aws configure set aws_secret_access_key "$SECRET_ACCESS_KEY"
echo "aws credentials configured."

S3_OBJ="${DESTINATION_BUCKET%/}/$KEY"

MAX_RETRIES=3
RETRY_DELAY=10
attempt=1
success=false

while [ $attempt -le $MAX_RETRIES ]; do
    echo "Download/upload attempt $attempt of $MAX_RETRIES"

    MD5_FILE="$(mktemp /tmp/md5.XXXXXX)"
    SIZE_FILE="$(mktemp /tmp/size.XXXXXX)"

    aws_cp_cmd=(aws s3 cp - "$S3_OBJ")
    if [ -n "${SIZE:-}" ]; then
        aws_cp_cmd+=(--expected-size "$SIZE")
    fi

    if curl --fail --location "https://api.gdc.cancer.gov/data/$ID" \
         --header "X-Auth-Token: $GDC_TOKEN" \
        | tee >(md5sum | awk '{print $1}' > "$MD5_FILE") \
        | tee >(wc -c | awk '{print $1}' > "$SIZE_FILE") \
        | "${aws_cp_cmd[@]}"; then

        echo "Stream to S3 completed, validating size and md5..."

        downloaded_size="$(cat "$SIZE_FILE")"
        downloaded_md5="$(cat "$MD5_FILE")"

        rm -f "$MD5_FILE" "$SIZE_FILE"

        if [ -n "${SIZE:-}" ] && [ "$downloaded_size" -ne "$SIZE" ]; then
            echo "Size mismatch: Expected $SIZE bytes, got $downloaded_size bytes"
            echo "Removing possibly corrupt S3 object: $S3_OBJ"
            aws s3 rm "$S3_OBJ" || true
        elif [ -n "${MD5SUM:-}" ] && [ "$downloaded_md5" != "$MD5SUM" ]; then
            echo "md5sum mismatch: Expected $MD5SUM, got $downloaded_md5"
            echo "Removing possibly corrupt S3 object: $S3_OBJ"
            aws s3 rm "$S3_OBJ" || true
        else
            echo "Download validation passed:"
            echo "  Size: $downloaded_size (expected $SIZE)"
            echo "  MD5 : $downloaded_md5 (expected $MD5SUM)"
            echo "Upload Complete"
            success=true
            break
        fi
    else
        echo "curl/pipe/aws s3 cp pipeline failed"
        rm -f "$MD5_FILE" "$SIZE_FILE"
    fi

    echo "Attempt $attempt failed, sleeping $RETRY_DELAY seconds then retrying..."
    sleep "$RETRY_DELAY"
    ((attempt++))
done

if [ "$success" = false ]; then
    echo "ERROR: File transfer failed after $MAX_RETRIES attempts"
    exit 1
fi

echo "SUCCESS: File verified and transferred"
exit 0
