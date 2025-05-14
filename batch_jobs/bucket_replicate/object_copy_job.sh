#/bin/bash

aws configure set aws_access_key_id $ACCESS_KEY_ID
aws configure set aws_secret_access_key $SECRET_ACCESS_KEY

/bucket-replicate/mount-s3 $DESTINATION_BUCKET ./mnt

# Copy the file locally from the GDC bucket
echo "testfile" > ./mnt/$KEY
# <gdc download> ./mnt/$KEY
