#/bin/bash
echo "Starting the job.."
set -uxo pipefail  # Enable strict error handling
# https://gist.github.com/mohanpedala/1e2ff5661761d3abd0385e8223e16425?permalink_comment_id=3935570#set--e--u--x--o-pipefail

remount_bucket_run_cmd () {
    MOUNT_RETRIES=3
    MOUNT_DELAY=5 # seconds

    for i in $(seq 1 $MOUNT_RETRIES); do
        echo "Remount s3 bucket.."
        umount ./mnt
        sleep 2
        # Mount S3 bucket
        mount-s3 --allow-overwrite --allow-delete $1 ./mnt
        echo "attempt to run command.."
        eval "$2"
        if [ $? -eq 0 ]; then
            echo "Command run successfully."
            break
        else
            echo "command failed. Sleep then try again"
            sleep $MOUNT_DELAY
        fi
        if [ $i -eq $MOUNT_RETRIES ]; then
            echo "Mount failed after $MOUNT_RETRIES attempts."
            exit 1
        fi
    done
}

aws configure set aws_access_key_id $ACCESS_KEY_ID
aws configure set aws_secret_access_key $SECRET_ACCESS_KEY
echo "aws credentials configured."

# Mount S3 bucket
mount-s3 --allow-overwrite --allow-delete $DESTINATION_BUCKET ./mnt
sleep 2
echo "s3 bucket mounted."

# Create directory structure
mkdir -p "./mnt/$(dirname "$KEY")"
echo "created directory for object."

# Download file with retries and validation
MAX_RETRIES=3
RETRY_DELAY=10
attempt=1
success=false

while [ $attempt -le $MAX_RETRIES ]; do
    echo "Download attempt $attempt of $MAX_RETRIES"

    # Download the file
    curl --fail --location "https://api.gdc.cancer.gov/data/$ID" \
         --header "X-Auth-Token: $GDC_TOKEN" \
         --output "./mnt/$KEY"
         #TODO This option returns 405
         #--data '{"stream": true}'

    # Verify download success
    if [ $? -eq 0 ]; then
        # Verify file size
        downloaded_size=$(stat -c%s "./mnt/$KEY")
        if [ "$downloaded_size" -eq "$SIZE" ]; then
            echo "Download validation passed: Size matches expected $SIZE bytes"
            #TODO: Put md5sum check back!!
            #Calculate MD5 checksum for additional validation
            # if command -v md5sum >/dev/null 2>&1; then
            #     cmd="md5sum "./mnt/$KEY" | cut -d' ' -f1"
            #     downloaded_md5=$(eval $cmd)
            #     if [ $? -ne 0 ]; then
            #         remount_bucket_run_cmd $DESTINATION_BUCKET "downloaded_md5=$(eval $cmd)"
            #     fi
            #     if [ "$downloaded_md5" = "$MD5SUM" ]; then
            #         echo "Download validation passed, md5 is $downloaded_md5"
            #     else
            #         rm -f "./mnt/$KEY"
            #         echo "md5sum mismatch: Expected $MD5SUM, got $downloaded_md5"
            #     fi
            # fi
            echo "Upload Complete"
            success=true
            umount "./mnt"
            sleep 2
            break
        else
            rm -f "./mnt/$KEY"
            echo "Size mismatch: Expected $SIZE bytes, got $downloaded_size bytes"
        fi
    fi

    # Cleanup failed attempt
    sleep $RETRY_DELAY
    ((attempt++))
done

if [ "$success" = false ]; then
    echo "ERROR: File transfer failed after $MAX_RETRIES attempts"
    exit 1
fi

echo "SUCCESS: File verified and transferred"
exit 0
