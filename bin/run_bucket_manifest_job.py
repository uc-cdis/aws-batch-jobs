
import argparse
from bucket_manifest.bucket_manifest_job import purge_queue, list_objects, bucket_manifest_job
from bucket_manifest.utils import write_messages_to_tsv


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    bucket_manifest_cmd = subparsers.add_parser("create_manifest")
    bucket_manifest_cmd.add_argument("--bucket", required=True, help="s3 bucket name to generate manifest for")
    bucket_manifest_cmd.add_argument("--job_queue", required=True, help="The name of s3 job queue")
    bucket_manifest_cmd.add_argument("--job_definition", required=True, help="The name of the job definition")
    bucket_manifest_cmd.add_argument("--sqs", required=True, help="The name of SQS")
    bucket_manifest_cmd.add_argument("--out_bucket", required=True, help="The name of the bucket which the output manifest is put to")

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    if args.action == "create_manifest":
        purge_queue(args.sqs)
        keys = list_objects(args.bucket)
        bucket_manifest_job.submit_jobs(args.job_queue, args.job_definition, keys)
        write_messages_to_tsv(args.sqs, len(keys), args.out_bucket)