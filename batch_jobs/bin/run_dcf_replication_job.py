import argparse
import settings

from batch_jobs.dcf_replication.dcf_replication_job import run_job


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    dcf_replication_cmd = subparsers.add_parser("dcf_replication")
    dcf_replication_cmd.add_argument(
        "--manifest_path",
        required=True,
        help="s3 path to the manifest file that contains the list of files to be copied",
    )
    dcf_replication_cmd.add_argument(
        "--thread_count",
        required=True,
        help="Thread pool for number of concurrent files copied",
    )
    dcf_replication_cmd.add_argument(
        "--max_retries",
        required=False,
        default=10,
        help="Max retry attempt until a file copy is considered a failed",
    )

    dcf_replication_cmd.add_argument(
        "--job_queue", required=True, help="The name of s3 job queue"
    )
    dcf_replication_cmd.add_argument(
        "--job_definition", required=True, help="The name of the job definition"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    if args.action == "dcf_replication":
        run_job(
            args.manifest_path,
            args.job_queue,
            args.job_definition,
            args.thread_count,
            args.max_retries,
        )
