import pytest

from .test_encode_file_transfer import MockResponse


def test_encode_portal_helper_server_init(server):
    from encode_file_transfer import EncodePortalHelper
    eph = EncodePortalHelper(server)
    assert eph.server == 'https://encode-demo.org'


def test_encode_server_is_indexing(server, mocker):
    import requests
    from encode_file_transfer import EncodePortalHelper
    mocker.patch('requests.get')
    requests.get.return_value = MockResponse(
        {'status': 'indexing'},
        200,
        text=''
    )
    eph = EncodePortalHelper(server)
    assert eph.is_indexing()


def test_encode_server_is_not_indexing(server, mocker):
    import requests
    from encode_file_transfer import EncodePortalHelper
    mocker.patch('requests.get')
    requests.get.return_value = MockResponse(
        {'status': 'waiting'},
        200,
        text=''
    )
    eph = EncodePortalHelper(server)
    assert not eph.is_indexing()


def test_encode_portal_helper_get_connection_error(server, mocker):
    import requests
    from requests.exceptions import ConnectionError
    from encode_file_transfer import EncodePortalHelper
    mocker.patch('requests.get')
    requests.get.side_effect = ConnectionError()
    eph = EncodePortalHelper(server)
    with pytest.raises(ConnectionError):
        eph.is_indexing()


def test_encode_portal_helper_get_status_code_404_no_search_results(server, mocker, no_search_results):
    import requests
    from encode_file_transfer import EncodePortalHelper
    mocker.patch('requests.get')
    requests.get.return_value = MockResponse(
        no_search_results,
        404,
        text=''
    )
    eph = EncodePortalHelper(server)
    parsed_metadata = eph.get_file_metadata()
    assert len(parsed_metadata) == 0


def test_encode_portal_helper_get_status_code_not_200(server, mocker):
    import requests
    from encode_file_transfer import EncodePortalHelper
    mocker.patch('requests.get')
    requests.get.return_value = MockResponse(
        {},
        422,
        text='Access denied'
    )
    eph = EncodePortalHelper(server)
    with pytest.raises(ValueError):
        eph.is_indexing()


def test_encode_portal_helper_patch_server(server, mocker):
    import requests
    from encode_file_transfer import EncodePortalHelper
    mocker.patch('requests.patch')
    requests.patch.return_value = MockResponse(
        {'new_bucket': 'new_test_bucket', 'old_bucket': 'old_test_bucket'},
        200,
        text=''
    )
    eph = EncodePortalHelper(server)
    r = eph._patch(server, {'new_bucket': 'new-test-bucket'}, creds=('ABC', '123'))
    assert r.json()['new_bucket'] == 'new_test_bucket'
    assert r.json()['old_bucket'] == 'old_test_bucket'


def test_encode_portal_helper_patch_connection_error(server, mocker):
    import requests
    from requests.exceptions import ConnectionError
    from encode_file_transfer import EncodePortalHelper
    mocker.patch('requests.patch')
    requests.patch.side_effect = ConnectionError()
    eph = EncodePortalHelper(server)
    with pytest.raises(ConnectionError):
        eph._patch(server, {'new_bucket': 'new-test-bucket'}, creds=('ABC', '123'))


def test_encode_portal_helper_patch_status_code_not_200(server, mocker):
    import requests
    from encode_file_transfer import EncodePortalHelper
    mocker.patch('requests.patch')
    requests.patch.return_value = MockResponse(
        {},
        422,
        text='Access denied'
    )
    eph = EncodePortalHelper(server)
    with pytest.raises(ValueError):
        eph._patch(server, {'new_bucket': 'new-test-bucket'}, creds=('ABC', '123'))


def test_encode_portal_helper_make_audit_query(server):
    from encode_file_transfer import EncodePortalHelper
    eph = EncodePortalHelper(server)
    assert eph._make_audit_query() == 'https://encode-demo.org/search/?type=File&audit.INTERNAL_ACTION.category=incorrect+file+bucket&format=json'


def test_encode_portal_helper_make_audit_query_with_batch(server):
    from encode_file_transfer import EncodePortalHelper
    eph = EncodePortalHelper(server)
    assert eph._make_audit_query(batch_size=100) == 'https://encode-demo.org/search/?type=File&audit.INTERNAL_ACTION.category=incorrect+file+bucket&format=json&limit=100'


def test_encode_portal_helper_make_audit_query_with_query_filter(server):
    from encode_file_transfer import EncodePortalHelper
    eph = EncodePortalHelper(server)
    assert eph._make_audit_query(query_filter={'status': 'in progress'}) == 'https://encode-demo.org/search/?type=File&audit.INTERNAL_ACTION.category=incorrect+file+bucket&format=json&status=in+progress'


def test_encode_portal_helper_make_audit_query_with_batch_and_query_filter(server):
    from encode_file_transfer import EncodePortalHelper
    eph = EncodePortalHelper(server)
    audit_query = eph._make_audit_query(
        batch_size=3,
        query_filter={
            'accession': [
                'ENCFF000123',
                'ENCFF000456'
            ]
        }
    )
    assert audit_query == 'https://encode-demo.org/search/?type=File&audit.INTERNAL_ACTION.category=incorrect+file+bucket&format=json&limit=3&accession=ENCFF000123&accession=ENCFF000456'


def test_encode_portal_helper_parse_audits(server, search_results):
    from encode_file_transfer import EncodePortalHelper
    eph = EncodePortalHelper(server)
    parsed_audits = eph._parse_audits(search_results)
    assert parsed_audits == [
        (
            '/files/ENCFF077CVI/',
            'Move in progress file ENCFF077CVI from s3://encode-files/2019/02/11/fb0cc28e-ddb9-44c6-a597-69f71a75d6a8/ENCFF077CVI.bigBed to s3://encode-pds-private-dev/2019/02/11/fb0cc28e-ddb9-44c6-a597-69f71a75d6a8/ENCFF077CVI.bigBed'
        ),
        (
            '/files/ENCFF910LZK/',
            'Move in progress file ENCFF910LZK from s3://encode-files/2019/02/11/0e18d1ba-4804-4ca8-9e8e-65eb640b9908/ENCFF910LZK.bigBed to s3://encode-pds-private-dev/2019/02/11/0e18d1ba-4804-4ca8-9e8e-65eb640b9908/ENCFF910LZK.bigBed'
        )
    ]


def test_encode_portal_helper_get_files_in_incorrect_bucket(server, search_results, mocker):
    import requests
    from encode_file_transfer import EncodePortalHelper
    mocker.patch('requests.get')
    requests.get.return_value = MockResponse(
        {'@graph': search_results},
        200,
        text=''
    )
    eph = EncodePortalHelper(server)
    parsed_audits = eph._parse_audits(search_results)
    assert parsed_audits == [
        (
            '/files/ENCFF077CVI/',
            'Move in progress file ENCFF077CVI from s3://encode-files/2019/02/11/fb0cc28e-ddb9-44c6-a597-69f71a75d6a8/ENCFF077CVI.bigBed to s3://encode-pds-private-dev/2019/02/11/fb0cc28e-ddb9-44c6-a597-69f71a75d6a8/ENCFF077CVI.bigBed'
        ),
        (
            '/files/ENCFF910LZK/',
            'Move in progress file ENCFF910LZK from s3://encode-files/2019/02/11/0e18d1ba-4804-4ca8-9e8e-65eb640b9908/ENCFF910LZK.bigBed to s3://encode-pds-private-dev/2019/02/11/0e18d1ba-4804-4ca8-9e8e-65eb640b9908/ENCFF910LZK.bigBed'
        )
    ]


def test_encode_portal_helper_parse_query_filter(server):
    from encode_file_transfer import EncodePortalHelper
    eph = EncodePortalHelper(server)
    pqf = eph._parse_query_filter({'accession': 'ENCFF000123'})
    assert pqf == 'accession=ENCFF000123'
    pqf = eph._parse_query_filter({'accession': ['ENCFF000123', 'ENCFF000456']})
    assert pqf == 'accession=ENCFF000123&accession=ENCFF000456'


def test_encode_portal_helper_make_metadata_query(server):
    from encode_file_transfer import EncodePortalHelper
    eph = EncodePortalHelper(server)
    metadata_query = eph._make_metadata_query()
    assert all(
        x in metadata_query
        for x in [
                'https://encode-demo.org/search/?type=File',
                '&format=json',
                '&status=released',
                '&status=archived',
                '&restricted%21=%2A',
                '&field=s3_uri',
                '&field=azure_uri',
                '&field=status',
                '&field=file_format',
                '&field=accession',
                '&field=dataset',
                '&field=lab.%40id',
                '&field=assembly',
                '&field=output_category',
                '&field=file_size'
                '&field=date_created',
                '&field=file_type',
                '&field=replicate_libraries',
                '&field=md5sum',
                '&field=cloud_metadata.url',
                '&field=cloud_metadata.md5sum_base64',
                '&field=award.rfa',
                '&field=analysis_step_version.analysis_step.name',
                '&no_file_available=false',
                '&audit.INTERNAL_ACTION.category%21=incorrect+file+bucket',
        ]
    )


def test_encode_portal_helper_make_metadata_query_with_batch(server):
    from encode_file_transfer import EncodePortalHelper
    eph = EncodePortalHelper(server, batch_size='all')
    metadata_query = eph._make_metadata_query()
    assert '&limit=all' in metadata_query


def test_encode_portal_helper_flatten_json(server, metadata_results):
    from encode_file_transfer import EncodePortalHelper
    eph = EncodePortalHelper(server)
    flattened_data = eph._flatten_json(metadata_results[0])
    expected = {
        'cloud_metadata.url': 'https://encode-files.s3.amazonaws.com/2019/02/22/ffca661c-38b2-4470-8411-b5d724f84303/ENCFF525YUW.vcf.gz',
        'md5sum': '19e37212a2b8a24a1cfd12ffd50ef257',
        's3_uri': 's3://encode-files/2019/02/22/ffca661c-38b2-4470-8411-b5d724f84303/ENCFF525YUW.vcf.gz',
        "azure_uri": "https://datasetencode.blob.core.windows.net/dataset/2019/02/22/ffca661c-38b2-4470-8411-b5d724f84303/ENCFF525YUW.vcf.gz?sv=2019-10-10&si=prod&sr=c&sig=9qSQZo4ggrCNpybBExU8SypuUZV33igI11xw0P7rB3c%3D",
        'assembly': 'hg19',
        'lab.@id': '/labs/alexander-urban/',
        'file_size': 504017,
        'dataset': '/experiments/ENCSR276ECO/',
        'output_category': 'annotation',
        'file_format': 'vcf',
        'status': 'released',
        'award.rfa': 'community',
        'cloud_metadata.md5sum_base64': 'GeNyEqK4okoc/RL/1Q7yVw==',
        'analysis_step_version.analysis_step.name': 'genotyping-hts-bam-to-vcf-arcsv-step-v-1',
        'file_type': 'vcf',
        'accession': 'ENCFF525YUW',
        'date_created': '2019-02-22T16:59:32.376723+00:00',
        'replicate_libraries': ['/libraries/ENCLB553KIK/']
    }
    for k, v in expected.items():
        assert flattened_data[k] == v


def test_encode_portal_helper_parse_metadata(server, metadata_results):
    from encode_file_transfer import EncodePortalHelper
    eph = EncodePortalHelper(server)
    parsed_metadata = eph._parse_metadata(metadata_results)
    assert len(parsed_metadata) == 2
    urls = sorted([x['cloud_metadata.url'] for x in parsed_metadata])
    assert urls == [
        'https://encode-files.s3.amazonaws.com/2019/02/15/cb1979da-3628-4e76-8449-98e7df1ccd5d/ENCFF322LPX.bam',
        'https://encode-files.s3.amazonaws.com/2019/02/22/ffca661c-38b2-4470-8411-b5d724f84303/ENCFF525YUW.vcf.gz'
    ]


def test_encode_portal_helper_get_file_metadata(server, mocker, metadata_results):
    import requests
    from encode_file_transfer import EncodePortalHelper
    mocker.patch('requests.get')
    requests.get.return_value = MockResponse(
        {'@graph': metadata_results},
        200,
        text=''
    )
    eph = EncodePortalHelper(server)
    parsed_metadata = eph.get_file_metadata()
    assert len(parsed_metadata) == 2
    labs = sorted([x['lab.@id'] for x in parsed_metadata])
    assert labs == [
        '/labs/alexander-urban/',
        '/labs/encode-processing-pipeline/'
    ]


def test_encode_portal_helper_zero_search_results_true(server, mocker, no_search_results):
    import requests
    from encode_file_transfer import EncodePortalHelper
    eph = EncodePortalHelper(server)
    mocker.patch('requests.get')
    requests.get.return_value = MockResponse(
        no_search_results,
        404,
        text=''
    )
    r = requests.get('https://no_results_found.com')
    assert eph._zero_search_results(r)


def test_encode_portal_helper_zero_search_results_false(server, mocker, no_search_results):
    import requests
    from encode_file_transfer import EncodePortalHelper
    eph = EncodePortalHelper(server)
    mocker.patch('requests.get')
    requests.get.return_value = MockResponse(
        {},
        404,
        text=''
    )
    r = requests.get('https://no_results_found.com')
    assert not eph._zero_search_results(r)
