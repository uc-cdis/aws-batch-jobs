#/bin/bash

aws configure set aws_access_key_id $ACCESS_KEY_ID
aws configure set aws_secret_access_key $SECRET_ACCESS_KEY

mount-s3 --allow-overwrite $DESTINATION_BUCKET ./mnt

mkdir -p "./mnt/$(dirname "$KEY")"

# Copy the file locally from the GDC bucket
#echo "testfile" > ./mnt/$KEY


# <gdc download> ./mnt/$KEY
curl --location 'https://api.gdc.cancer.gov/data/'$ID'' \
--header 'X-Auth-Token: '$GDC_TOKEN'' \
--output ./mnt/$KEY
