import argparse
import settings

from batch_jobs.dcf_bucket_replicate.dcf_bucket_replicate_job import run_job


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    bucket_manifest_cmd = subparsers.add_parser("dcf-replicate-bucket")
    bucket_manifest_cmd.add_argument(
        "--source_bucket",
        required=True,
        help="source s3 bucket where data will be copied from",
    )
    bucket_manifest_cmd.add_argument(
        "--manifest",
        required=True,
        help="a manifest (tsv) of files to replicate. Required colums: project_id, url",
    )
    bucket_manifest_cmd.add_argument(
        "--mapping",
        required=True,
        help="a json file that maps project_id to target bucket",
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
    if args.action == "dcf-replicate-bucket":
        run_job(
            args.source_bucket,
            args.manifest,
            args.mapping,
            args.job_queue,
            args.job_definition,
        )
