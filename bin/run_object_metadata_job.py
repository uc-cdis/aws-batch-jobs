import os
from bucket_manifest.object_metadata_job import compute_object_metadata

SQS_NAME = os.environ.get("SQS_NAME")
if __name__ == "__main__":
    compute_object_metadata(SQS_NAME)