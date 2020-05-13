import pytest
import bucket_manifest

def test1(monkeypatch):
    import pdb; pdb.set_trace()
    monkeypatch.setattr('bucket_manifest.object_metadata_job.ACCESS_KEY_ID', "ACCESS_KEY_ID")
    bucket_manifest.object_metadata_job.compute_object_metadata("")
    pass