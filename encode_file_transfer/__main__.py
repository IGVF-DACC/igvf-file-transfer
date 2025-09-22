import boto3
from encode_file_transfer import EncodeFileTransfer
from encode_file_transfer.interface import (
    SERVER,
    AWS_DEFAULT_REGION,
    PORTAL_CREDS,
    AWS_CREDS,
    DEFAULT_MAIN_ARG
)
import sys
import os


def get_run_type():
    args = sys.argv
    try:
        return args[1]
    except IndexError:
        return DEFAULT_MAIN_ARG


def main():
    run_type = get_run_type()
    print('Running as {}'.format(run_type))
    eft = EncodeFileTransfer(
        SERVER,
        batch_size='all',
        portal_creds=(os.environ['PORTAL_KEY'], os.environ['PORTAL_SECRET_KEY']),
        aws_creds=(os.environ['ACCESS_KEY'], os.environ['SECRET_ACCESS_KEY']),
    )
    if run_type == 'sync':
        eft.sync_buckets_and_portal()
    elif run_type == 'metadata':
        eft.dump_file_metadata_to_s3()


if __name__ == '__main__':
    main()
