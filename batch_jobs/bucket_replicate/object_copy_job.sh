#/bin/bash


env
echo $ACCESS_KEY_ID
echo $SECRET_ACCESS_KEY

aws configure set aws_access_key_id $ACCESS_KEY_ID
aws configure set aws_secret_access_key $SECRET_ACCESS_KEY

aws s3 cp s3://$SOURCE_BUCKET/$KEY s3://$DESTINATION_BUCKET/$KEY
