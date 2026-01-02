#!/bin/bash
set -uxo pipefail

# Comment this out when we move to using just aws creds account
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

    MD5_FILE="$(mktemp /tmp/md5.XXXXXX)"
    SIZE_FILE="$(mktemp /tmp/size.XXXXXX)"

    aws_cp_cmd=(aws s3 cp - "$S3_OBJ")
    if [ -n "${SIZE:-}" ]; then
        aws_cp_cmd+=(--expected-size "$SIZE")
    fi

    if [ -n "${PROFILE_NAME:-}" ]; then
        aws_cp_cmd+=(--profile "$PROFILE_NAME")

    if curl --fail --location "https://api.gdc.cancer.gov/data/$ID" \
             --header "X-Auth-Token: $GDC_TOKEN" \
        | tee >(md5sum | awk '{print $1}' > "$MD5_FILE") \
        | tee >(wc -c    | awk '{print $1}' > "$SIZE_FILE") \
        | "${aws_cp_cmd[@]}"; then


        downloaded_size="$(cat "$SIZE_FILE")"
        downloaded_md5="$(cat "$MD5_FILE")"

        rm -f "$MD5_FILE" "$SIZE_FILE"

        size_ok=true
        md5_ok=true

        if [ -n "${SIZE:-}" ]; then
            if [ "$downloaded_size" -ne "$SIZE" ]; then
                echo "Size mismatch: Expected $SIZE bytes, got $downloaded_size bytes"
                size_ok=false
            else
                echo "Size validation passed: $downloaded_size bytes"
            fi
        else
            echo "SIZE not set, skipping size validation (observed: $downloaded_size bytes)"
        fi

        if [ -n "${MD5SUM:-}" ]; then
            if [ "$downloaded_md5" != "$MD5SUM" ]; then
                echo "md5sum mismatch: Expected $MD5SUM, got $downloaded_md5"
                md5_ok=false
            else
                echo "MD5 validation passed: $downloaded_md5"
            fi
        else
            echo "MD5SUM not set, skipping MD5 validation (observed: $downloaded_md5)"
        fi

        if [ "$size_ok" = true ] && [ "$md5_ok" = true ]; then
            echo "Download validation passed, upload complete."
            success=true
            break
        else
            echo "Validation failed, removing possibly corrupt S3 object: $S3_OBJ"
            aws s3 rm "$S3_OBJ" || true
        fi

    else
        echo "curl/pipe/aws s3 cp pipeline failed"
        rm -f "$MD5_FILE" "$SIZE_FILE" || true
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
