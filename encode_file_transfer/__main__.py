import argparse
import os
import ast

import boto3
from encode_file_transfer import EncodeFileTransfer
from encode_file_transfer.interface import (
    SERVER,
    AWS_DEFAULT_REGION,
    PORTAL_CREDS,
    AWS_CREDS,
    DEFAULT_MAIN_ARG
)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'run_type',
        nargs='?',
        choices=['sync', 'metadata'],
        default=DEFAULT_MAIN_ARG,
        help='Type of operation to run (sync or metadata)'
    )
    parser.add_argument(
        '--query-filter',
        required=False,
        type=str,
        help='Query filter as JSON string: \"{\'file_size\': \'gt:2300\'}\"'
    )
    parser.add_argument(
        '--portal-key',
        default=os.environ.get('PORTAL_KEY'),
        help='Portal key (default: PORTAL_KEY environment variable)'
    )
    parser.add_argument(
        '--portal-secret-key',
        default=os.environ.get('PORTAL_SECRET_KEY'),
        help='Portal secret key (default: PORTAL_SECRET_KEY environment variable)'
    )
    # AWS credentials
    parser.add_argument(
        '--access-key',
        default=os.environ.get('ACCESS_KEY'),
        help='AWS access key (default: ACCESS_KEY environment variable)'
    )
    parser.add_argument(
        '--secret-access-key',
        default=os.environ.get('SECRET_ACCESS_KEY'),
        help='AWS secret access key (default: SECRET_ACCESS_KEY environment variable)'
    )
    parser.add_argument(
        '--batch-size',
        default='all',
        help='Batch size for processing',
    )
    args = parser.parse_args()
    if not args.portal_key or not args.portal_secret_key:
        raise ValueError('Portal credentials must be provided via environment variables or command line arguments')
    if not args.access_key or not args.secret_access_key:
        raise ValueError('AWS credentials must be provided via environment variables or command line arguments')
    if args.query_filter:
        args.query_filter = ast.literal_eval(args.query_filter)
    if args.batch_size.isdecimal():
        args.batch_size = int(args.batch_size)
    return args


def main():
    args = get_args()
    print('Args', args)
    run_type = args.run_type
    print('Running as {}'.format(run_type))
    eft = EncodeFileTransfer(
        SERVER,
        batch_size=args.batch_size,
        portal_creds=(args.portal_key, args.portal_secret_key),
        aws_creds=(args.access_key, args.secret_access_key),
        query_filter=args.query_filter,
    )
    if run_type == 'sync':
        eft.sync_buckets_and_portal()
    elif run_type == 'metadata':
        eft.dump_file_metadata_to_s3()


if __name__ == '__main__':
    main()
