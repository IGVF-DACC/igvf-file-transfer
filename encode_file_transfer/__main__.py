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


def get_encrypted_parameter(name):
    ssm = boto3.client('ssm', region_name=AWS_DEFAULT_REGION)
    res = ssm.get_parameter(Name=name, WithDecryption=True)
    return res.get('Parameter', {}).get('Value')


def get_portal_creds():
    return eval(get_encrypted_parameter(PORTAL_CREDS))


def get_aws_creds():
    return eval(get_encrypted_parameter(AWS_CREDS))


def get_run_type():
    args = sys.argv
    try:
        return args[1]
    except IndexError:
        return DEFAULT_MAIN_ARG


def main():
    run_type = get_run_type()
    print('Running as {}'.format(run_type))
    portal_creds = get_portal_creds()
    aws_creds = get_aws_creds()
    eft = EncodeFileTransfer(
        SERVER,
        batch_size='all',
        portal_creds=portal_creds,
        aws_creds=aws_creds,
    )
    if run_type == 'sync':
        eft.sync_buckets_and_portal()
    elif run_type == 'metadata':
        eft.dump_file_metadata_to_s3()


if __name__ == '__main__':
    main()
