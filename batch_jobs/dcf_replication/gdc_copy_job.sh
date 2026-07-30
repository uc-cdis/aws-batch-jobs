#!/bin/bash
set -uxo pipefail

echo_with_job_id() {
    echo "File $ID: $1"
}

aws configure set aws_access_key_id "$ACCESS_KEY_ID"
aws configure set aws_secret_access_key "$SECRET_ACCESS_KEY"
echo_with_job_id "aws credentials configured."

if [[ "$DESTINATION_BUCKET" == s3://* ]]; then
    S3_PREFIX="${DESTINATION_BUCKET%/}"
else
    S3_PREFIX="s3://$DESTINATION_BUCKET"
fi

MB_MULTIPLIER=$(( 1024 ** 2 ))
MULTI_PART_THRESHOLD=$(( $MB_MULTIPLIER * $MULTI_PART_THRESHOLD ))
CHUNK_SIZE=$(( $MB_MULTIPLIER * $CHUNK_SIZE ))

S3_OBJ="$S3_PREFIX/$KEY"
MAX_RETRIES=3
RETRY_DELAY=10
attempt=1
success=false

if [ "$SIZE" -ge "$MULTI_PART_THRESHOLD" ]; then

    echo_with_job_id "Using multipart to upload file $ID..."

    command="python3 ./batch_jobs/dcf_replication/file_get_upload.py upload_data --file_id $ID --gdc_token $GDC_TOKEN --target_bucket $DESTINATION_BUCKET --object_path $KEY --file_size $SIZE --expected_md5 $MD5SUM --chunk_size $CHUNK_SIZE"

    while [ "$attempt" -le "$MAX_RETRIES" ]; do
        if $command; then
            echo_with_job_id "Download validation passed, upload complete."
            success=true
            break
        else
            echo_with_job_id "Transfer failed on attempt $attempt"
        fi

        echo_with_job_id "Attempt $attempt failed, sleeping $RETRY_DELAY seconds then retrying..."
        sleep "$RETRY_DELAY"
        attempt=$((attempt + 1))
    done

    if [ "$success" = false ]; then
        echo_with_job_id "ERROR: File transfer failed after $MAX_RETRIES attempts"
        exit 1
    fi

    echo_with_job_id "SUCCESS: File verified and transferred"
else
    echo_with_job_id "Using s5cmd to upload file $ID..."

    while [ "$attempt" -le "$MAX_RETRIES" ]; do
        HASH_FILE="$(mktemp /tmp/hashout.XXXXXX)"
        S5CMD_ERR_FILE="$(mktemp /tmp/s5cmderr.XXXXXX)"

        aws_cp_cmd=(s5cmd pipe "$S3_OBJ")

        if [ -n "${PROFILE_NAME:-}" ]; then
            export AWS_PROFILE="$PROFILE_NAME"
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
    sys.exit(1)" 2>"$HASH_FILE" \
            | "${aws_cp_cmd[@]}" 2>"$S5CMD_ERR_FILE"; then

            downloaded_md5=$(sed -n '1p' "$HASH_FILE")
            downloaded_size=$(sed -n '2p' "$HASH_FILE")
            rm -f "$HASH_FILE" "$S5CMD_ERR_FILE"

            size_ok=true
            md5_ok=true

            if [ -n "${SIZE:-}" ]; then
                if [ "$downloaded_size" -ne "$SIZE" ]; then
                    echo_with_job_id "Size mismatch: Expected $SIZE bytes, got $downloaded_size bytes"
                    size_ok=false
                else
                    echo_with_job_id "Size validation passed: $downloaded_size bytes"
                fi
            else
                echo_with_job_id "SIZE not set, skipping size validation (observed: $downloaded_size bytes)"
            fi

            if [ -n "${MD5SUM:-}" ]; then
                if [ "$downloaded_md5" != "$MD5SUM" ]; then
                    echo_with_job_id "md5sum mismatch: Expected $MD5SUM, got $downloaded_md5"
                    md5_ok=false
                else
                    echo_with_job_id "MD5 validation passed: $downloaded_md5"
                fi
            else
                echo_with_job_id "MD5SUM not set, skipping MD5 validation (observed: $downloaded_md5)"
            fi

            if [ "$size_ok" = true ] && [ "$md5_ok" = true ]; then
                echo_with_job_id "Download validation passed, upload complete."
                success=true
                break
            else
                echo_with_job_id "Validation failed, removing possibly corrupt S3 object: $S3_OBJ"
                s5cmd rm "$S3_OBJ" || true
            fi

        else
            echo_with_job_id "curl/pipe/s5cmd pipeline failed"
            echo_with_job_id "=== Python stderr (md5/size or traceback) ==="
            cat "$HASH_FILE" || true
            echo_with_job_id "=== s5cmd stderr ==="
            cat "$S5CMD_ERR_FILE" || true
            rm -f "$HASH_FILE" "$S5CMD_ERR_FILE" || true
        fi

        echo_with_job_id "Attempt $attempt failed, sleeping $RETRY_DELAY seconds then retrying..."
        sleep "$RETRY_DELAY"
        attempt=$((attempt + 1))
    done

    if [ "$success" = false ]; then
        echo_with_job_id "ERROR: File transfer failed after $MAX_RETRIES attempts"
        exit 1
    fi

    echo_with_job_id "SUCCESS: File verified and transferred"
fi

if [ "$success" = true ]; then
    echo "Start Indexing File"
    index_command="python3 ./batch_jobs/dcf_replication/index_file.py index_record --host $HOSTNAME --username $USERNAME --password $PASSWORD --file_id $ID --s3_url $S3_OBJ --file_size $SIZE --md5_hash $MD5SUM --acl $ACL --authz $AUTHZ --retry $MAX_RETRIES"
    $index_command
fi

exit 0
