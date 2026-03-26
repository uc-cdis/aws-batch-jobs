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
    HASH_FILE="$(mktemp /tmp/hashout.XXXXXX)"
    AWS_ERR_FILE="$(mktemp /tmp/awserr.XXXXXX)"

    aws_cp_cmd=(s5cmd pipe "$S3_OBJ")

    if [ -n "${PROFILE_NAME:-}" ]; then
        aws_cp_cmd+=(--credentials-file ~/.aws/credentials --profile "$PROFILE_NAME")
    fi

    if curl --fail --location "https://api.gdc.cancer.gov/data/$ID" \
        --header "X-Auth-Token: $GDC_TOKEN" \
        | python3 -c "
import sys, hashlib, traceback
try:
    h = hashlib.md5()
    n = 0
    buf_size = 8 * 1024 * 1024
    while True:
        chunk = sys.stdin.buffer.read(buf_size)
        if not chunk:
            break
        h.update(chunk)
        n += len(chunk)
        sys.stdout.buffer.write(chunk)
        sys.stdout.buffer.flush()
    sys.stderr.write(h.hexdigest() + '\n')
    sys.stderr.write(str(n) + '\n')
except Exception as e:
    sys.stderr.write('PYTHON ERROR: ' + str(e) + '\n')
    sys.stderr.write(traceback.format_exc())
    sys.exit(1)
" 2>"$HASH_FILE" \
        | "${aws_cp_cmd[@]}" 2>"$AWS_ERR_FILE"; then

        downloaded_md5=$(sed -n '1p' "$HASH_FILE")
        downloaded_size=$(sed -n '2p' "$HASH_FILE")
        rm -f "$HASH_FILE" "$AWS_ERR_FILE"

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
            s5cmd rm "$S3_OBJ" || true
        fi

    else
        echo "curl/pipe/s5cmd pipeline failed"
        echo "=== Python stderr (md5/size or traceback) ==="
        cat "$HASH_FILE" || true
        echo "=== s5cmd stderr ==="
        cat "$AWS_ERR_FILE" || true
        rm -f "$HASH_FILE" "$AWS_ERR_FILE" || true
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
