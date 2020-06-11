import argparse
import settings

from batch_jobs.bucket_manifest.bucket_manifest_job import run_job


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    bucket_manifest_cmd = subparsers.add_parser("create_manifest")
    bucket_manifest_cmd.add_argument(
        "--bucket", required=True, help="s3 bucket name to generate manifest for"
    )
    bucket_manifest_cmd.add_argument(
        "--job_queue", required=True, help="The name of s3 job queue"
    )
    bucket_manifest_cmd.add_argument(
        "--job_definition", required=True, help="The name of the job definition"
    )
    bucket_manifest_cmd.add_argument("--sqs", required=True, help="The name of SQS")
    bucket_manifest_cmd.add_argument(
        "--out_bucket",
        required=True,
        help="The name of the bucket which the output manifest is put to",
    )
    bucket_manifest_cmd.add_argument(
        "--authz", required=False, help="The file contains authz"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    if args.action == "create_manifest":
        run_job(
            args.bucket,
            args.job_queue,
            args.job_definition,
            args.sqs,
            args.out_bucket,
            args.authz,
        )
