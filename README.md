# aws-batch-jobs
Place to store all aws batch jobs

## Bucket-manifest

`object_metadata_job.py` is a script to compute object metadata
`bucket_manifest_job.py` is a script to submit jobs to the job queue and consume the SQS

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
In order to run an aws batch job, you need to spin up an AWS infrastructure including an job queue, an job definition and a computational environment. You can manually create the infrastructure with AWS CLI, or terraform. In Gen3, you can use the command `gen3 workon <namespace> <prefix>__batch` in adminVM

The output of the batch job are stored in an SQS queue which is consumed by the script `bucket_manifest_job.py`. 
An example of the body of a SQS message:
```
{
  "Body": {"url": "s3://test_bucket/test_key, "size": 100, "md5": "91afa59a9469e12e0b0844712c756b08"}
}
```

