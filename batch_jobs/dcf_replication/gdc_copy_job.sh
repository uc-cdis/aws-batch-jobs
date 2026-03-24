#!/bin/bash
set -uxo pipefail

# Comment this out when we move to using just aws creds account
aws configure set aws_access_key_id "$ACCESS_KEY_ID"
aws configure set aws_secret_access_key "$SECRET_ACCESS_KEY"
aws configure set default.s3.multipart_chunksize 64MB
aws configure set default.s3.max_concurrent_requests 1
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
AWS_ERR_FILE="$(mktemp /tmp/awserr.XXXXXX)"

#TODO: Remove this. This one is just for testing purposes.
ID="51508da5-018b-4393-808a-88a92b843667"
FILE_NAME="largefile_3.dat"
SIZE="536870912000"
MD5SUM="9887d53e52060668fd266480a2f3ad73" #pragma: allowlist secret
DESTINATION_BUCKET="log-bucket-june2"
KEY="final_destination.dat"
S3_OBJ="s3://test-gdc-abc-phs000222-2-open/final_file99.dat"

while [ "$attempt" -le "$MAX_RETRIES" ]; do
    MD5_FILE="$(mktemp /tmp/md5.XXXXXX)"
    SIZE_FILE="$(mktemp /tmp/size.XXXXXX)"

    aws_cp_cmd=(aws s3 cp - "$S3_OBJ")
    if [ -n "${SIZE:-}" ]; then
        aws_cp_cmd+=(--expected-size "$SIZE")
    fi
    if [ -n "${PROFILE_NAME:-}" ]; then
        aws_cp_cmd+=(--profile "$PROFILE_NAME")
    fi

    if curl --fail --location "$CURL_LOCATION" \
            | tee >(md5sum | awk '{print $1}' > "$MD5_FILE") \
            | tee >(wc -c  | awk '{print $1}' > "$SIZE_FILE") \
            | "${aws_cp_cmd[@]}" 2>"$AWS_ERR_FILE"; then

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
        cat "$MD5_FILE"
        cat "$SIZE_FILE"
        rm -f "$MD5_FILE" "$SIZE_FILE" || true
        echo "=== AWS CLI stderr ==="
        cat "$AWS_ERR_FILE" || true
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
