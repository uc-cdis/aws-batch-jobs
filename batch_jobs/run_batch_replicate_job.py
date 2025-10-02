import argparse

from batch_jobs.dcf_replicate.dcf_replication import run_job


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    bucket_manifest_cmd = subparsers.add_parser("replicate-bucket")
    bucket_manifest_cmd.add_argument(
        "--gdc_manifest_path",
        required=True,
        help="s3 path to the manifest file that contains the list of files to be copied",
    )
    bucket_manifest_cmd.add_argument(
        "--job_queue", required=True, help="The name of s3 job queue"
    )
    bucket_manifest_cmd.add_argument(
        "--job_definition", required=True, help="The name of the job definition"
    )
    bucket_manifest_cmd.add_argument(
        "--destination_bucket", required=True, help="Destination Bucket Name"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    if args.action == "replicate-bucket":
        run_job(
            args.gdc_manifest_path,
            args.job_queue,
            args.job_definition,
            args.destination_bucket,
        )
