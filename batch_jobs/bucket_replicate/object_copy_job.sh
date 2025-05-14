#/bin/bash

aws configure set aws_access_key_id $ACCESS_KEY_ID
aws configure set aws_secret_access_key $SECRET_ACCESS_KEY

mount-s3 $DESTINATION_BUCKET ./mnt

mkdir -p "$(dirname "$DESTINATION_BUCKET")"

# Copy the file locally from the GDC bucket
echo "testfile" > ./mnt/$KEY
# <gdc download> ./mnt/$KEY
