import os
import pytest
from batch_jobs.bucket_replicate.dcf_replication import parse_manifest_file


def test_map_project_to_bucket():
    tsv_dir = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data/GDC_test_manifest.tsv"
    )
    print(parse_manifest_file(tsv_dir))
