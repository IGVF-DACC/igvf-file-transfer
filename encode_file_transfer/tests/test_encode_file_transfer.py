import pytest


class MockResponse():

        def __init__(self, json_data, status_code, text):
            self.json_data = json_data
            self.status_code = status_code
            self.text = text

        def json(self):
            return self.json_data


def test_encode_file_transfer_init(server):
    from encode_file_transfer import EncodeFileTransfer
    eft = EncodeFileTransfer(server)
    assert eft.server == 'https://encode-demo.org'


def test_encode_file_transfer_parse_s3_to_bucket_key(server, original_bucket_s3_uri):
    from encode_file_transfer import EncodeFileTransfer
    eft = EncodeFileTransfer(server)
    parsed_s3uri = eft._parse_s3_to_bucket_and_key(original_bucket_s3_uri)
    assert parsed_s3uri.get('Bucket') == 'encode-files'
    assert parsed_s3uri.get('Key') == '2019/02/11/fb0cc28e-ddb9-44c6-a597-69f71a75d6a8/ENCFF077CVI.bigBed'


def test_encode_file_transfer_parse_audit_details_for_source_and_destination(server, parsed_audit):
    from encode_file_transfer import EncodeFileTransfer
    eft = EncodeFileTransfer(server)
    parsed_audit_details = eft._parse_audit_details_for_source_and_destination(parsed_audit)
    assert parsed_audit_details.get('source_bucket') == 'encode-files'
    assert parsed_audit_details.get('source_key') == '2019/02/11/0e18d1ba-4804-4ca8-9e8e-65eb640b9908/ENCFF910LZK.bigBed'
    assert parsed_audit_details.get('destination_bucket') == 'encode-pds-private-dev'
    assert parsed_audit_details.get('destination_key') == '2019/02/11/0e18d1ba-4804-4ca8-9e8e-65eb640b9908/ENCFF910LZK.bigBed'


def test_encode_file_transfer_get_files_to_move(server, search_results, mocker):
    import requests
    from encode_file_transfer import EncodeFileTransfer
    mocker.patch('requests.get')
    requests.get.return_value = MockResponse(
        {'@graph': search_results},
        200,
        text=''
    )
    eft = EncodeFileTransfer(server)
    files_to_move = eft._get_files_to_move()
    assert len(files_to_move) == 2
    assert files_to_move[0].get('accession') == '/files/ENCFF077CVI/'
    assert files_to_move[0].get('source_bucket') == 'encode-files'
    assert files_to_move[0].get('source_key') == '2019/02/11/fb0cc28e-ddb9-44c6-a597-69f71a75d6a8/ENCFF077CVI.bigBed'
    assert files_to_move[0].get('destination_bucket') == 'encode-pds-private-dev'
    assert files_to_move[0].get('destination_key') == '2019/02/11/fb0cc28e-ddb9-44c6-a597-69f71a75d6a8/ENCFF077CVI.bigBed'
    assert files_to_move[1].get('accession') == '/files/ENCFF910LZK/'
    assert files_to_move[1].get('source_bucket') == 'encode-files'
    assert files_to_move[1].get('source_key') == '2019/02/11/0e18d1ba-4804-4ca8-9e8e-65eb640b9908/ENCFF910LZK.bigBed'
    assert files_to_move[1].get('destination_bucket') == 'encode-pds-private-dev'
    assert files_to_move[1].get('destination_key') == '2019/02/11/0e18d1ba-4804-4ca8-9e8e-65eb640b9908/ENCFF910LZK.bigBed'


def test_encode_file_transfer_make_bucket_update_url(server):
    from encode_file_transfer import EncodeFileTransfer
    eft = EncodeFileTransfer(server)
    accession = '/files/ENCFF000123/'
    url = eft._make_bucket_update_url(accession)
    assert url == 'https://encode-demo.org/files/ENCFF000123/@@update_bucket'


def test_encode_file_transfer_update_bucket_on_portal(server, mocker, file_to_move):
    import requests
    from encode_file_transfer import EncodeFileTransfer
    mocker.patch('requests.patch')
    requests.patch.return_value = MockResponse(
        json_data={
            'status': 'success',
            'old_bucket': 'encode-files',
            'new_bucket': 'encode-pds-public-dev',
            '@type': ['result']
        },
        status_code=200,
        text=''
    )
    eft = EncodeFileTransfer(server, portal_creds=('ABC', '123'))
    r = eft._update_bucket_on_portal(file_to_move)
    assert r.json().get('new_bucket') == 'encode-pds-public-dev'
    assert r.json().get('old_bucket') == 'encode-files'


def test_encode_file_transfer_wait_for_indexer_indexing(server, mocker):
    from encode_file_transfer import EncodeFileTransfer
    from encode_file_transfer import EncodePortalHelper
    eft = EncodeFileTransfer(server)
    mocker.patch('encode_file_transfer.EncodePortalHelper.is_indexing')
    EncodePortalHelper.is_indexing.return_value = True
    assert not eft._wait_for_indexer(times=[0, 1, 2])


def test_encode_file_transfer_wait_for_indexer_waiting(server, mocker):
    from encode_file_transfer import EncodeFileTransfer
    from encode_file_transfer import EncodePortalHelper
    eft = EncodeFileTransfer(server)
    mocker.patch('encode_file_transfer.EncodePortalHelper.is_indexing')
    EncodePortalHelper.is_indexing.return_value = False
    assert eft._wait_for_indexer(times=[0, 2, 4])


def test_encode_file_transfer_initial_transfer(server):
    from encode_file_transfer import EncodeFileTransfer
    eft = EncodeFileTransfer(server)
    assert not eft.initial_transfer
    eft = EncodeFileTransfer(server, initial_transfer=True)
    assert eft.initial_transfer


def test_encode_file_transfer_set_source_to_encode_public(server, mocker, file_to_move):
    from encode_file_transfer import EncodeFileTransfer
    from encode_file_transfer import s3Helper
    eft = EncodeFileTransfer(server)
    mocker.patch('encode_file_transfer.s3Helper._file_exists')
    s3Helper._file_exists.return_value = True
    assert file_to_move['source_bucket'] == 'encode-files'
    file_to_move = eft._set_source_to_encode_public(file_to_move)
    assert file_to_move['source_bucket'] == 'igvf-public'


def test_encode_file_transfer_deterimine_source_file_exists_in_source(server, mocker, file_to_move):
    from encode_file_transfer import EncodeFileTransfer
    from encode_file_transfer import s3Helper
    eft = EncodeFileTransfer(server)
    mocker.patch('encode_file_transfer.s3Helper._file_exists')
    s3Helper._file_exists.side_effect = [True, False, False]
    assert file_to_move['source_bucket'] == 'encode-files'
    file_to_move = eft._determine_source(file_to_move)
    assert file_to_move['source_bucket'] == 'encode-files'


def test_encode_file_transfer_deterimine_source_file_exists_in_destination(server, mocker, file_to_move):
    from encode_file_transfer import EncodeFileTransfer
    from encode_file_transfer import s3Helper
    eft = EncodeFileTransfer(server)
    mocker.patch('encode_file_transfer.s3Helper._file_exists')
    s3Helper._file_exists.side_effect = [False, True, False]
    assert file_to_move['source_bucket'] == 'encode-files'
    file_to_move = eft._determine_source(file_to_move)
    assert file_to_move['source_bucket'] == 'encode-pds-public-dev'


def test_encode_file_transfer_file_does_not_exist_in_source_or_destination(server, mocker, file_to_move):
    from encode_file_transfer import EncodeFileTransfer
    from encode_file_transfer import s3Helper
    eft = EncodeFileTransfer(server)
    mocker.patch('encode_file_transfer.s3Helper._file_exists')
    s3Helper._file_exists.side_effect = [False, False, True]
    file_to_move['source_bucket'] = 'not-encode-files'
    assert file_to_move['source_bucket'] == 'not-encode-files'
    file_to_move = eft._determine_source(file_to_move)
    assert file_to_move['source_bucket'] == 'igvf-files'
