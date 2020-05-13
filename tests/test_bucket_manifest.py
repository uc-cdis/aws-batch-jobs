import pytest
from batch_jobs.bucket_manifest.object_metadata_job import compute_object_metadata


def test1(monkeypatch):

    monkeypatch.setattr(
        "batch_jobs.bucket_manifest.object_metadata_job.ACCESS_KEY_ID", "ACCESS_KEY_ID"
    )
    monkeypatch.setattr(
        "batch_jobs.bucket_manifest.object_metadata_job.SECRET_ACCESS_KEY",
        "SECRET_ACCESS_KEY",
    )
    pass
