"""
"""
import sys
import os
import boto3
import logging
import hashlib
from urllib.parse import unquote_plus


logging.basicConfig(filename="manifest_ingestion.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

CHUNK_SIZE = 1024 * 1024 * 10
ACCESS_KEY_ID = os.environ["ACCESS_KEY_ID"]
SECRET_ACCESS_KEY = os.environ["SECRET_ACCESS_KEY"]
BUCKET = os.environ["BUCKET"]
S3KEY = os.environ["KEY"]


# a file containing a "guid" column and additional, arbitrary columns to populate
# into the metadata service

def main():
    md5_hash = hashlib.md5()
    s3Client = boto3.client('s3', aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY)
    try:
        # Assume it will succeed for now
        response = s3Client.get_object(
          Bucket=BUCKET,
          Key=unquote_plus(S3KEY)
        )
        res = response["Body"]
        data = res.read(CHUNK_SIZE)
        while data:
            md5_hash.update(data)
            data = res.read(CHUNK_SIZE)
        output = {"md5":  md5_hash.hexdigest(), "size": response['ContentLength']}
    except Exception as e:
        # If we run into any exceptions, fail this task so batch operations does not retry it and
        # return the exception string so we can see the failure message in the final report
        # created by batch operations.
        raise
    finally:
        pass

    sqs = boto3.resource('sqs', region_name="us-east-1")
    # Get the queue. This returns an SQS.Queue instance
    queue = sqs.get_queue_by_name(QueueName='terraform-example-queue')

    # You can now access identifiers and attributes
    response = queue.send_message(MessageBody='{}'.format(md5_hash.hexdigest()))

    
if __name__ == "__main__":
    main()
