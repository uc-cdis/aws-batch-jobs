aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID_SQS
aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY_SQS
aws s3 cp s3://$TEMP_BUCKET/sa.json ./sa.json

gcloud auth activate-service-account --key-file sa.json
gcloud config set project $PROJECT
mkdir file
gsutil -u $PROJECT cp gs://$SOURCE_BUCKET/$KEY ./file/
fileName=$(ls file)
md5=$(md5sum file/$fileName)
size=$(wc -c file/$fileName)
local QUEUE_URL=$(aws sqs get-queue-url --queue-name=$SQS_NAME |jq -r .QueueUrl)
#send sqs message
aws sqs send-message --region us-east-1 --endpoint-url https://sqs.us-east-1.amazonaws.com/ --queue-url $QUEUE_URL --message-body "{'url': gs://$SOURCE_BUCKET/$KEY, 'md5': $md5, 'size': $size}"

aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID_CLEVERSAFE
aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY_CLEVERSAFE

aws s3 cp ./file/$fileName s3://$DESTINATION_BUCKET/$KEY --endpoint-url=https://bionimbus-objstore-cs.opensciencedatacloud.org/