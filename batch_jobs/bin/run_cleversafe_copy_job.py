import argparse
import settings

from batch_jobs.copy_to_cleversafe.copy_bucket import run_job


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    cleversafe_copy_cmd = subparsers.add_parser("cleversafe-copy")
    cleversafe_copy_cmd.add_argument(
        "--source_bucket",
        required=True,
        help="source s3 bucket where data will be copied from",
    )
    cleversafe_copy_cmd.add_argument(
        "--destination_bucket",
        required=True,
        help="destination s3 bucket where data will be copied to",
    )
    cleversafe_copy_cmd.add_argument(
        "--job_queue", required=True, help="The name of s3 job queue"
    )
    cleversafe_copy_cmd.add_argument(
        "--job_definition", required=True, help="The name of the job definition"
    )
    cleversafe_copy_cmd.add_argument(
        "--project", required=True, help="The name of the job definition"
    )
    cleversafe_copy_cmd.add_argument("--sqs", required=True, help="The name of SQS")
    cleversafe_copy_cmd.add_argument(
        "--out_bucket",
        required=True,
        help="The name of the bucket which the output manifest is put to",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    if args.action == "cleversafe-copy":
        run_job(
            args.source_bucket,
            args.job_queue,
            args.job_definition,
            args.sqs,
            args.out_bucket,
            args.project,
        )