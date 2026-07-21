import argparse
import logging
import sys

from indexclient.client import IndexClient


def update_or_index_file(
    host,
    username,
    password,
    file_id,
    s3_url,
    file_size,
    md5_hash,
    acl,
    authz,
    retry,
):
    """
    Update or POST new IndexD Record with the gen3sdk


    Args:
        host (str): hostname of the data commons
        username (str): username for Basic Auth for IndexD API
        password (str): password for Basic Auth for IndexD API
        file_id (str): guid provided by the GDC manifest
        s3_url (str): s3 url of where the data is located
        file_size (int): file size from GDC manfiest
        md5_hash (str): md5 from the GDC manifest
        acl (str): acl from the GDC manifest
        authz (str): authz from the GDC manifest
        retry (int): number of retries for the api call
    """
    index_get_tries = 0
    index_post_tries = 0

    indexclient = IndexClient(
        host,
        "v0",
        (username, password),
    )

    print("====================")
    print(host, username, password, file_id, s3_url, file_size, md5_hash, acl, authz)

    # Make gdc https url for given record
    gdc_url = f"https://api.gdc.cancer.gov/data/{file_id}"
    input_urls = [s3_url, gdc_url]

    # record already exists
    while index_get_tries < retry:
        logging.info(f"Checking file {file_id} exists...")

        try:
            doc = indexclient.get(file_id)
            print("-------------------------------------------")
            print(doc)
            if doc:
                need_update = False
                if input_urls not in doc.urls:
                    doc.urls = input_urls
                    need_update = True

                if set(doc.acl) != set(acl):
                    doc.acl = acl
                    need_update = True

                if set(doc.authz) != set(authz):
                    doc.authz = authz
                    need_update = True

                if need_update:
                    doc.patch()
                break
        except Exception as e:
            index_get_tries += 1
            raise logging.error(
                "INDEX_CLIENT: Can not update the record with uuid {}. Detail {}".format(
                    file_id, e
                )
            )

    # record doesn't exist. Create a new one
    while index_post_tries < retry:
        try:
            doc = indexclient.create(
                did=file_id,
                hashes={"md5": md5_hash},
                size=file_size,
                acl=acl,
                authz=authz,
                urls=input_urls,
            )
            break
        except Exception as e:
            index_post_tries += 1
            raise logging.error(
                "INDEX_CLIENT: Can not create the record with uuid {}. Detail {}".format(
                    file_id, e
                )
            )


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(title="action", dest="action")

    index_record_cmd = subparser.add_parser("index_record")

    index_record_cmd.add_argument(
        "--host",
        required=True,
        help="host url of the commons that you want your record indexed to",
    )
    index_record_cmd.add_argument(
        "--username", required=True, help="Username for IndexD Basic Auth"
    )
    index_record_cmd.add_argument(
        "--password", required=True, help="Password for IndexD Basic Auth"
    )
    index_record_cmd.add_argument(
        "--file_id",
        required=True,
        help="uuid for the file to be copied",
    )
    index_record_cmd.add_argument(
        "--s3_url",
        required=True,
        help="Path at which the object will be uploaded to in the destination bucket",
    )
    index_record_cmd.add_argument(
        "--file_size",
        required=True,
        help="File size as defined in the GDC manifest. This will be used to validate uplaod",
    )
    index_record_cmd.add_argument(
        "--md5_hash",
        required=True,
        help="MD5 as given in the GDC manifest. This will be used to validate upload",
    )
    index_record_cmd.add_argument(
        "--acl", required=True, help="ACL as given in the GDC manifest."
    )
    index_record_cmd.add_argument(
        "--authz", required=True, help="Authz as given in the GDC manifest."
    )
    index_record_cmd.add_argument(
        "--retry",
        required=False,
        default=3,
        help="Number of retries for both download and upload",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    if args.action == "index_record":
        try:
            update_or_index_file(
                args.host,
                args.username,
                args.password,
                args.file_id,
                args.s3_url,
                int(args.file_size),
                args.md5_hash,
                args.acl,
                args.authz,
                int(args.retry),
            )
            logging.ingo(f"Successfully indexed file: {args.file_id}")
            sys.exit(0)
        except Exception as e:
            logging.error(f"Something went wrong with indexing: {e}")
            sys.exit(1)
