import argparse
import settings

from batch_jobs.bucket_replicate.bucket_replicate_job import run_job


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    bucket_manifest_cmd = subparsers.add_parser("replicate-bucket")
    bucket_manifest_cmd.add_argument(
        "--source_bucket", required=True, help="source s3 bucket where data will be copied from"
    )
    bucket_manifest_cmd.add_argument(
        "--destination_bucket", required=True, help="destination s3 bucket where data will be copied to"
    )
    bucket_manifest_cmd.add_argument(
        "--job_queue", required=True, help="The name of s3 job queue"
    )
    bucket_manifest_cmd.add_argument(
        "--job_definition", required=True, help="The name of the job definition"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    if args.action == "replicate-bucket":
        run_job(
            args.source_bucket,
            args.destinatino_bucket,
            args.job_queue,
            args.job_definition,
        )
