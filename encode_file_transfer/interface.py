SERVER = 'https://www.encodeproject.org'
PORTAL_CREDS = 'portal_creds'
DEFAULT_MAIN_ARG = 'sync'
AWS_CREDS = 'user_creds'
AWS_DEFAULT_REGION = 'us-west-2'
# URLSPLIT of full query.
SPLITQUERYTEMPLATE = [
    'https',
    '',
    '/search/',
    'type=File&audit.INTERNAL_ACTION.category=incorrect+file+bucket&format=json',
    ''
]
PORTAL_CREDS = 'portal_creds'
BUCKET_UPDATE = '{}/@@update_bucket'
ORIGINAL_BUCKET = 'encode-files'
PUBLIC_BUCKET = 'encode-public'
PRIVATE_BUCKET = 'encode-private'
INDEXER = '_indexer'
INDEXING_STATUS = 'indexing'
AUDIT = 'audit'
AUDIT_TYPE = 'INTERNAL_ACTION'
AUDIT_CATEGORY = 'incorrect file bucket'
BATCH_SIZE = 10
LOGFILE = 'transfer_log_{}.txt'
LOGBUCKET = 'encoded-logs'
LOGFOLDER = 'file-transfer-logs'
METADATA_TSV = 'encode_file_manifest.tsv'

FILE_METADATA_FIELDS = [
    'accession',
    'status',
    'file_format',
    'file_type',
    'assembly',
    'award.rfa',
    's3_uri',
    'azure_uri',
    'cloud_metadata.url',
    'dataset',
    'lab.@id',
    'output_type',
    'output_category',
    'file_size',
    'date_created',
    'md5sum',
    'cloud_metadata.md5sum_base64',
    'replicate_libraries',
    'analysis_step_version.analysis_step.name',
]
FILE_METADATA_QUERY_TEMPLATE = [
    'https',
    '',
    '/search/',
    'type=File&format=json',
    ''
]
FILE_METADATA_STATUSES = [
    'released',
    'archived',
]

GLACIER_TAG_KEY = 'copied_to'
GLACIER_TAG_VALUE = 'open_data_account'
GLACIER_TAG_SET = {
    'TagSet': [
        {
            'Key': GLACIER_TAG_KEY,
            'Value': GLACIER_TAG_VALUE
        }
    ]
}
