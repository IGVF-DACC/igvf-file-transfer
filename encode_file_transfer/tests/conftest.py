import pytest


@pytest.fixture
def server():
    return 'https://encode-demo.org'


@pytest.fixture
def search_results():
    return [
        {
            'audit': {
                'INTERNAL_ACTION': [
                    {
                        'level': 30,
                        'category': 'incorrect file bucket',
                        'name': 'audit_file',
                        'level_name': 'INTERNAL_ACTION',
                        'detail': 'Move in progress file ENCFF077CVI from s3://encode-files/2019/02/11/fb0cc28e-ddb9-44c6-a597-69f71a75d6a8/ENCFF077CVI.bigBed to s3://encode-pds-private-dev/2019/02/11/fb0cc28e-ddb9-44c6-a597-69f71a75d6a8/ENCFF077CVI.bigBed',
                        'path': "/files/ENCFF077CVI/"
                    }
                ]
            },
            '@id': '/files/ENCFF077CVI/',
        },
        {
            '@id': '/files/ENCFF910LZK/',
            'audit': {
                'INTERNAL_ACTION': [
                    {
                        'level': 30,
                        'category': 'incorrect file bucket',
                        'name': 'audit_file',
                        'level_name': 'INTERNAL_ACTION',
                        'detail': 'Move in progress file ENCFF910LZK from s3://encode-files/2019/02/11/0e18d1ba-4804-4ca8-9e8e-65eb640b9908/ENCFF910LZK.bigBed to s3://encode-pds-private-dev/2019/02/11/0e18d1ba-4804-4ca8-9e8e-65eb640b9908/ENCFF910LZK.bigBed',
                        'path': '/files/ENCFF910LZK/'
                    }
                ]
            }
        }
    ]


@pytest.fixture
def no_search_results():
    return {
        "sort": {
            "date_created": {
                "unmapped_type": "keyword", "order": "desc"
            },
            "label": {
                "unmapped_type": "keyword", "missing": "_last", "order": "asc"
            }
        },
        "clear_filters": "/search/?type=File",
        "total": 0,
        "notification": "No results found",
        "@type": ["Search"],
        "title": "Search",
        "@graph": [ ],
    }


@pytest.fixture
def original_bucket_s3_uri():
    return 's3://encode-files/2019/02/11/fb0cc28e-ddb9-44c6-a597-69f71a75d6a8/ENCFF077CVI.bigBed'


@pytest.fixture
def private_dev_bucket_s3_uri():
    return 's3://encode-pds-private-dev/2019/02/11/fb0cc28e-ddb9-44c6-a597-69f71a75d6a8/ENCFF077CVI.bigBed'


@pytest.fixture
def public_dev_bucket_s3_uri():
    return 's3://encode-pds-public-dev/2019/02/11/fb0cc28e-ddb9-44c6-a597-69f71a75d6a8/ENCFF077CVI.bigBed'


@pytest.fixture
def parsed_audit():
    return (
        '/files/ENCFF910LZK/',
        'Move in progress file ENCFF910LZK from s3://encode-files/2019/02/11/0e18d1ba-4804-4ca8-9e8e-65eb640b9908/ENCFF910LZK.bigBed to s3://encode-pds-private-dev/2019/02/11/0e18d1ba-4804-4ca8-9e8e-65eb640b9908/ENCFF910LZK.bigBed'
    )


@pytest.fixture
def file_to_move():
    return {
        'destination_bucket': 'encode-pds-public-dev',
        'accession': '/files/ENCFF321OXI/',
        'source_bucket': 'encode-files',
        'source_key': '2019/02/09/dc1388a0-7a81-4255-8de1-a1bd186208f8/ENCFF321OXI.bigBed',
        'destination_key': '2019/02/09/dc1388a0-7a81-4255-8de1-a1bd186208f8/ENCFF321OXI.bigBed'
    }


@pytest.fixture
def metadata_results():
    return [
        {
            "accession": "ENCFF525YUW",
            "@id": "/files/ENCFF525YUW/",
            "assembly": "hg19",
            "cloud_metadata": {
                "md5sum_base64": "GeNyEqK4okoc/RL/1Q7yVw==",
                "url": "https://encode-files.s3.amazonaws.com/2019/02/22/ffca661c-38b2-4470-8411-b5d724f84303/ENCFF525YUW.vcf.gz"
            },
            "@type": ["File", "Item"],
            "output_category": "annotation",
            "dataset": "/experiments/ENCSR276ECO/",
            "analysis_step_version": {
                "analysis_step": {
                    "name": "genotyping-hts-bam-to-vcf-arcsv-step-v-1"
                }
            },
            "date_created": "2019-02-22T16:59:32.376723+00:00",
            "md5sum": "19e37212a2b8a24a1cfd12ffd50ef257",
            "s3_uri": "s3://encode-files/2019/02/22/ffca661c-38b2-4470-8411-b5d724f84303/ENCFF525YUW.vcf.gz",
            "azure_uri": "https://datasetencode.blob.core.windows.net/dataset/2019/02/22/ffca661c-38b2-4470-8411-b5d724f84303/ENCFF525YUW.vcf.gz?sv=2019-10-10&si=prod&sr=c&sig=9qSQZo4ggrCNpybBExU8SypuUZV33igI11xw0P7rB3c%3D",
            "file_format": "vcf",
            "status": "released",
            "lab": {
                "@id": "/labs/alexander-urban/"
            },
            "replicate_libraries": ["/libraries/ENCLB553KIK/"],
            "award": {
                "rfa": "community"
            },
            "file_size": 504017,
            "file_type": "vcf"
        },
        {
            "accession": "ENCFF322LPX",
            "@id": "/files/ENCFF322LPX/",
            "assembly": "hg19",
            "cloud_metadata": {
                "md5sum_base64": "P+q1Jwib+FdHA1CyaFkL2A==",
                "url": "https://encode-files.s3.amazonaws.com/2019/02/15/cb1979da-3628-4e76-8449-98e7df1ccd5d/ENCFF322LPX.bam"
            },
            "@type": ["File", "Item"],
            "output_category": "alignment",
            "dataset": "/experiments/ENCSR000BVR/",
            "analysis_step_version": {
                "analysis_step": {
                    "name": "bwa-alignment-step-v-1"
                }
            },
            "date_created": "2019-02-15T21:33:59.374190+00:00",
            "md5sum": "3feab527089bf857470350b268590bd8",
            "s3_uri": "s3://encode-files/2019/02/15/cb1979da-3628-4e76-8449-98e7df1ccd5d/ENCFF322LPX.bam",
            "azure_uri": "https://datasetencode.blob.core.windows.net/dataset/2019/02/15/cb1979da-3628-4e76-8449-98e7df1ccd5d/ENCFF322LPX.bam?sv=2019-10-10&si=prod&sr=c&sig=9qSQZo4ggrCNpybBExU8SypuUZV33igI11xw0P7rB3c%3D",
            "file_format": "bam",
            "status": "released",
            "lab": {
                "@id": "/labs/encode-processing-pipeline/"
            },
            "replicate_libraries": ["/libraries/ENCLB559KDF/"],
            "award": {"rfa": "ENCODE3"},
            "file_size": 1670720764,
            "file_type": "bam"
        }
    ]
