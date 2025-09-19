SERVER = 'https://api.data.igvf.org'
PORTAL_CREDS = 'igvf-file-transfer-portal-creds'
DEFAULT_MAIN_ARG = 'sync'
AWS_CREDS = 'igvf-file-transfer-aws-creds'
AWS_DEFAULT_REGION = 'us-west-2'
# URLSPLIT of full query.
SPLITQUERYTEMPLATE = [
    'https',
    '',
    '/search/',
    'type=File&audit.INTERNAL_ACTION.category=incorrect+file+bucket',
    ''
]
BUCKET_UPDATE = '{}/@@update_bucket'
ORIGINAL_BUCKET = 'igvf-files'
PUBLIC_BUCKET = 'igvf-public'
PRIVATE_BUCKET = 'igvf-private'
INDEXER = 'indexer-info'
AUDIT = 'audit'
AUDIT_TYPE = 'INTERNAL_ACTION'
AUDIT_CATEGORY = 'incorrect file bucket'
BATCH_SIZE = 10
LOGFILE = 'transfer_log_{}.txt'
METADATA_TSV = 'igvf_file_manifest.tsv'

FILE_METADATA_FIELDS = [
    '@id',
    'href',
    'accession',
    'file_format',
    'file_format_type',
    'content_type',
    'summary',
    'file_set.accession',
    'file_set.file_set_type',
    'assay_titles',
    'preferred_assay_titles',
    'file_set.donors.accession',
    'file_set.samples.accession',
    'file_set.samples.sample_terms.term_name',
    'file_set.samples.summary',
    'cell_type_annotation.term_name',
    'creation_timestamp',
    'file_size',
    'file_set.lab.title',
    's3_uri',
    'assembly',
    'transcriptome_annotation',
    'controlled_access',
    'md5sum',
    'derived_from',
    'status',
    'upload_status',
    'flowcell_id',
    'lane',
    'sequencing_run',
    'illumina_read_type',
    'mean_read_length',
    'seqspecs',
    'seqspec_document',
    'sequencing_kit',
    'sequencing_platform.term_name',
    'workflows.accession'
]

FILE_METADATA_QUERY_TEMPLATE = [
    'https',
    '',
    '/search/',
    'type=File',
    ''
]

FILE_METADATA_STATUSES = [
    'released',
    'archived',
]

FILE_METADATA_UPLOAD_STATUSES = [
    'validated',
    'validation exempted',
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
