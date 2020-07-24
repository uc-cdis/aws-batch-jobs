# contents of our test file e.g. test_code.py
import sys
import json
import os
import pytest
import boto3
from moto import mock_sqs, mock_s3

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

fake_message1 = {
    "md5": "d9673f3128fcfbd70d040f7dc18afbd8",
    "size": 7,
    "url": "s3://test_bucket/test_key",
}

fake_message2 = {
    "md5": "d9673f3128fcfbd70d040f7dc18afaaa",
    "size": 11,
    "url": "s3://test_bucket/test_key2",
}


@pytest.fixture(scope="function")
def mock_env():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["BUCKET"] = "test_bucket"
    os.environ["S3KEY"] = "test_key"


@pytest.fixture(scope="function")
def create_mock_sqs():
    with mock_sqs():
        sqs = boto3.resource("sqs", region_name="us-east-1")
        queue = sqs.create_queue(QueueName="test", Attributes={"DelaySeconds": "0"})
        queue.send_message(MessageBody=json.dumps(fake_message1))
        queue.send_message(MessageBody=json.dumps(fake_message2))
        yield


@pytest.fixture(scope="function")
def s3(mock_env):
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test_bucket")
        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket="test_bucket", Key="test_key", Body="Awesome")
        yield
