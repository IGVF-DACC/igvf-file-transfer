import pytest


def test_s3_helper_parse_file_to_move(file_to_move):
    from encode_file_transfer import s3Helper
    s3h = s3Helper()
    sb, sk, db, dk = s3h._parse_file_to_move(file_to_move)
    assert sb == 'encode-files'
    assert sk == '2019/02/09/dc1388a0-7a81-4255-8de1-a1bd186208f8/ENCFF321OXI.bigBed'
    assert db == 'encode-pds-public-dev'
    assert dk == '2019/02/09/dc1388a0-7a81-4255-8de1-a1bd186208f8/ENCFF321OXI.bigBed'
