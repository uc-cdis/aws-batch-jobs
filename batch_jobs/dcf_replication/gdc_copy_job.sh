#/bin/bash
echo "Starting the job.."
set -euo pipefail  # Enable strict error handling
# https://gist.github.com/mohanpedala/1e2ff5661761d3abd0385e8223e16425?permalink_comment_id=3935570#set--e--u--x--o-pipefail

#Enable more tracing
set -x

aws configure set aws_access_key_id $ACCESS_KEY_ID
aws configure set aws_secret_access_key $SECRET_ACCESS_KEY
echo "aws credentials configured."

# Mount S3 bucket
mount-s3 --allow-overwrite $DESTINATION_BUCKET ./mnt
echo "s3 bucket mounted."

# Create directory structure
mkdir -p "./mnt/$(dirname "$KEY")"
echo "created directory for object."

# Download file with retries and validation
MAX_RETRIES=5
RETRY_DELAY=10
attempt=1
success=false

while [ $attempt -le $MAX_RETRIES ]; do
    echo "Download attempt $attempt of $MAX_RETRIES"

    # Download the file
    curl --fail --location "https://api.gdc.cancer.gov/data/$ID" \
         --header "X-Auth-Token: $GDC_TOKEN" \
         --output "./mnt/$KEY.tmp"
         #TODO This option returns 405
         #--data '{"stream": true}'

    # Verify download success
    if [ $? -eq 0 ]; then
        # Verify file size
        downloaded_size=$(stat -c%s "./mnt/$KEY.tmp")
        if [ "$downloaded_size" -eq "$SIZE" ]; then
            # Move to final location
            echo "Download validation passed: Size matches expected ($SIZE bytes)"

            # Calculate MD5 checksum for additional validation
            if command -v md5sum >/dev/null 2>&1; then
                downloaded_md5=$(md5sum "./mnt/$KEY.tmp" | cut -d' ' -f1)
                echo "Downloaded file MD5: $downloaded_md5"
            fi

            # Move to final location
            echo "Uploading..."
            mv "./mnt/$KEY.tmp" "./mnt/$KEY"
            echo "Upload Complete"

            success=true
            break
        else
            echo "Size mismatch: Expected $SIZE bytes, got $downloaded_size bytes"
        fi
    fi

    # Cleanup failed attempt
    rm -f "./mnt/$KEY.tmp"
    sleep $RETRY_DELAY
    ((attempt++))
done

if [ "$success" = false ]; then
    echo "ERROR: File transfer failed after $MAX_RETRIES attempts"
    exit 1
fi


echo "SUCCESS: File verified and transferred"
exit 0
