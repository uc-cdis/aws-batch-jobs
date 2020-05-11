# aws-batch-jobs
Place to store all aws batch jobs

## Bucket-manifest

`object_metadat_job.py` is a cript to compute object metadata
`bucket_manifesgt_job.py` is a script to submit jobs to the job queue and consume the SQS

The job definition is described below. 
```
  {
    "image": "quay.io/cdis/object_metadata:master",
    "memory": 256,
    "vcpus": 1,
    "environment": [
        {"name": "ACCESS_KEY_ID", "value": "${access_key}"},
        {"name": "SECRET_ACCESS_KEY", "value": "${secret_key}"},
        {"name": "BUCKET", "value": "${bucket}"},
        {"name": "SQS_NAME", "value": "${sqs_name}"}
    ]
}
```

The body of a SQS message:
```
{
  "Body": {"url": "s3://test_bucket/test_key, "size": 100, "md5": "91afa59a9469e12e0b0844712c756b08"}
}
```