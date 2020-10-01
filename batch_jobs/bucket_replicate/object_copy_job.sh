#/bin/bash

aws configure set aws_access_key_id $ACCESS_KEY_ID_ORIGINAL
aws configure set aws_secret_access_key $SECRET_ACCESS_KEY_ORIGINAL

aws s3api put-object-acl --bucket $bucket --key "$key" --acl bucket-owner-full-control

aws configure set aws_access_key_id $ACCESS_KEY_ID_DESTINATION
aws configure set aws_secret_access_key $SECRET_ACCESS_KEY_DESTINATION

aws s3 cp "s3://$bucket/$key"  "s3://$bucket/$key"  --metadata-directive REPLACE
