import csv
import logging
import boto3
from botocore.exceptions import ClientError


def write_tsv(filename, files, fieldnames=None):
    """
    write to tsv file
    Args:
        filename(str): file name
        files(list(dict)): list of file info
        [
            {
                "GUID": "guid_example",
                "filename": "example",
                "size": 100,
                "acl": "['open']",
                "md5": "md5_hash",
            },
        ]
        fieldnames(list(str)): list of column names
    Returns:
        filename(str): file name
    """

    if not files:
        return None
    fieldnames = fieldnames or files[0].keys()
    with open(filename, mode="w") as outfile:
        writer = csv.DictWriter(outfile, delimiter="\t", fieldnames=fieldnames)
        writer.writeheader()

        for f in files:
            for field in fieldnames:
                if field not in f:
                    f[field] = None
            try:
                writer.writerow(f)
            except Exception as e:
                logging.info(f"fieldnames {fieldnames}")
                logging.error(f)
                raise e

    return filename


def upload_file(
    file_name,
    bucket,
    object_name=None,
    aws_access_key_id=None,
    aws_secret_access_key=None,
):
    """
    Upload a file to an S3 bucket
    
    Args:
        file_name: File to upload
        bucket: Bucket to upload to
        object_name: S3 object name. If not specified then file_name is used
        aws_access_key_id: string
        aws_secret_access_key: string
    Returns:
        Bool: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    if aws_access_key_id and aws_secret_access_key:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
    else:
        s3_client = boto3.client("s3")

    try:
        msg = f"upload_file {file_name} in {bucket}, object: {object_name}"
        logging.info(msg)
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
