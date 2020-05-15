import pytest
from unittest.mock import MagicMock
from contextlib import contextmanager

import boto3
from botocore.stub import Stubber
from botocore.exceptions import ClientError

from batch_jobs.bucket_manifest.object_metadata_job import (
    compute_object_metadata,
    send_message,
)
from batch_jobs.bucket_manifest.bucket_manifest_job import (
    get_messages_from_queue,
    submit_job,
    list_objects,
)


@contextmanager
def mock_file(filepath, content=""):
    with open(filepath, "w") as f:
        f.write(content)
    yield filepath
    try:
        os.remove(filepath)
    except Exception:
        pass


def test_compute_object_metadata_success(monkeypatch, s3):

    monkeypatch.setattr(
        "batch_jobs.bucket_manifest.object_metadata_job.BUCKET", "test_bucket"
    )
    monkeypatch.setattr(
        "batch_jobs.bucket_manifest.object_metadata_job.S3KEY", "test_key"
    )

    monkeypatch.setattr("batch_jobs.bucket_manifest.object_metadata_job.MAX_RETRIES", 1)
    output = compute_object_metadata()
    assert output == {
        "url": "s3://test_bucket/test_key",
        "md5": "d9673f3128fcfbd70d040f7dc18afbd8",
        "size": 7,
    }


def test_compute_object_metadata_fail_due_to_wrong_key(monkeypatch, s3):

    monkeypatch.setattr(
        "batch_jobs.bucket_manifest.object_metadata_job.BUCKET", "test_bucket"
    )
    monkeypatch.setattr(
        "batch_jobs.bucket_manifest.object_metadata_job.S3KEY", "test_key2"
    )

    monkeypatch.setattr("batch_jobs.bucket_manifest.object_metadata_job.MAX_RETRIES", 1)
    output = compute_object_metadata()
    assert output == {
        "url": "s3://test_bucket/test_key2",
        "ERROR": "An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.",
    }


def test_send_message_success(create_mock_sqs):
    assert send_message("test", {})


def test_send_message_fail_due_to_queue_not_exist(create_mock_sqs):
    with pytest.raises(ClientError):
        send_message("test2", {})


def test_receive_message_from_sqs(create_mock_sqs):
    sqs = boto3.resource("sqs", region_name="us-east-1")
    queue = sqs.get_queue_by_name(QueueName="test")
    files = get_messages_from_queue(queue.url, 1)

    assert files == [
        {
            "md5": "d9673f3128fcfbd70d040f7dc18afbd1",
            "size": 10,
            "url": "s3://test_bucket/test_key",
        }
    ]


def test_list_object(s3):
    with mock_file(
        "./creds.json", '{"aws_access_key_id": "test", "aws_secret_access_key": "test"}'
    ):
        assert list_objects("test_bucket") == ["test_key"]


def test_submit_jobs_success(monkeypatch):
    monkeypatch.setattr("batch_jobs.bucket_manifest.bucket_manifest_job.MAX_RETRIES", 1)
    client = boto3.client("batch", region_name="us-east-1")
    stubber = Stubber(client)
    stubber.add_response(
        "submit_job", service_response={"jobName": "bucket_manifest", "jobId": "123"}
    )
    boto3.client = MagicMock()
    boto3.client.return_value = client
    with stubber:
        assert submit_job("test", "rest", "key")


def test_submit_jobs_fail(monkeypatch):
    monkeypatch.setattr("batch_jobs.bucket_manifest.bucket_manifest_job.MAX_RETRIES", 1)
    client = boto3.client("batch", region_name="us-east-1")
    stubber = Stubber(client)
    stubber.add_client_error("submit_job")
    boto3.client = MagicMock()
    boto3.client.return_value = client
    with stubber:
        assert submit_job("test", "rest", "key") == False
