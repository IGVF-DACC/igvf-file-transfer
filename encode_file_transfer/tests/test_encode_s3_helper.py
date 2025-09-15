import pytest
import boto3
from moto import mock_s3


def make_bucket(bucket, s3=None):
    if not s3:
        s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=bucket)
    return s3


def make_s3_object(s3, bucket, key):
    test_object = s3.Object(bucket, key)
    test_object.put(Body=b'test')


def make_file_to_move_bucket(objects_to_make):
    s3 = boto3.resource('s3')
    for bucket, key in objects_to_make:
        s3.create_bucket(Bucket=bucket)
        test_object = s3.Object(bucket, key)
        test_object.put(Body=b'test')
    return s3


def test_s3_helper_parse_file_to_move(file_to_move):
    from encode_file_transfer import s3Helper
    s3h = s3Helper()
    sb, sk, db, dk = s3h._parse_file_to_move(file_to_move)
    assert sb == 'encode-files'
    assert sk == '2019/02/09/dc1388a0-7a81-4255-8de1-a1bd186208f8/ENCFF321OXI.bigBed'
    assert db == 'encode-pds-public-dev'
    assert dk == '2019/02/09/dc1388a0-7a81-4255-8de1-a1bd186208f8/ENCFF321OXI.bigBed'


@mock_s3
def test_s3_helper_file_exists():
    s3 = make_bucket('my_test_bucket')
    make_s3_object(s3, 'my_test_bucket', 'my_test_key.tst')
    from encode_file_transfer import s3Helper
    s3h = s3Helper()
    assert s3h._file_exists('my_test_bucket', 'my_test_key.tst')


@mock_s3
def test_s3_helper_file_does_not_exists():
    s3 = make_bucket('my_test_bucket')
    make_s3_object(s3, 'my_test_bucket', 'my_test_key.tst')
    from encode_file_transfer import s3Helper
    s3h = s3Helper()
    assert not s3h._file_exists('my_test_bucket', 'my_missing_file.tst')


@mock_s3
def test_s3_helper_move_file(file_to_move):
    from encode_file_transfer import s3Helper
    s3h = s3Helper()
    sb, sk, db, dk = s3h._parse_file_to_move(file_to_move)
    # Make source bucket and key.
    s3 = make_bucket(sb)
    make_s3_object(s3, sb, sk)
    # Make just destination bucket.
    s3 = make_bucket(db, s3)
    assert s3h._move_file(file_to_move)


@mock_s3
def test_s3_helper_delete_file(file_to_move):
    from encode_file_transfer import s3Helper
    s3h = s3Helper()
    _, _, db, dk = s3h._parse_file_to_move(file_to_move)
    file_to_move = file_to_move.copy()
    s3 = make_bucket(db)
    make_bucket('test_bucket', s3)
    make_s3_object(s3, db, dk)
    # Set source to something other than encode-files or delete won't work.
    file_to_move['source_bucket'] = 'test_bucket'
    file_to_move['source_key'] = dk
    assert s3h._delete_file(file_to_move)


@mock_s3
def test_s3_helper_not_delete_file_when_source_and_destination_same(file_to_move):
    from encode_file_transfer import s3Helper
    s3h = s3Helper()
    _, _, db, dk = s3h._parse_file_to_move(file_to_move)
    file_to_move = file_to_move.copy()
    s3 = make_bucket(db)
    make_s3_object(s3, db, dk)
    file_to_move['source_bucket'] = db
    file_to_move['source_key'] = dk
    assert not s3h._delete_file(file_to_move)


@mock_s3
def test_s3_helper_no_delete_file_from_original_file(file_to_move):
    from encode_file_transfer import s3Helper
    s3h = s3Helper()
    sb, sk, db, dk = s3h._parse_file_to_move(file_to_move)
    print(sb)
    # Make source bucket and key.
    s3 = make_bucket(sb)
    make_s3_object(s3, sb, sk)
    # Make just destination bucket.
    s3 = make_bucket(db, s3)
    # Won't delete encode-files bucket.
    assert not s3h._delete_file(file_to_move)


@mock_s3
def test_s3_helper_tag_file(file_to_move):
    from encode_file_transfer import s3Helper
    s3h = s3Helper()
    sb, sk, db, dk = s3h._parse_file_to_move(file_to_move)
    file_to_move = file_to_move.copy()
    s3 = make_bucket(sb)
    make_bucket('test_bucket', s3)
    make_s3_object(s3, sb, sk)
    assert s3h._tag_file(file_to_move)
    tagset = s3.meta.client.get_object_tagging(
        Bucket=sb,
        Key=sk
    )['TagSet']
    assert tagset[0] == {
        'Key': 'copied_to',
        'Value': 'open_data_account'
    }


@mock_s3
def test_s3_helper_no_tag_file(file_to_move):
    from encode_file_transfer import s3Helper
    s3h = s3Helper()
    sb, sk, db, dk = s3h._parse_file_to_move(file_to_move)
    file_to_move = file_to_move.copy()
    s3 = make_bucket(sb)
    make_bucket('test_bucket', s3)
    make_s3_object(s3, sb, sk)
    file_to_move['source_bucket'] = db
    file_to_move['source_key'] = dk
    assert not s3h._tag_file(file_to_move)
    assert not s3.meta.client.get_object_tagging(
        Bucket=sb,
        Key=sk
    )['TagSet']


@mock_s3
def test_s3_helper_get_session():
    from encode_file_transfer import s3Helper
    s3h = s3Helper()
    session = s3h._get_session()
    assert isinstance(session, boto3.Session)


@mock_s3
def test_s3_helper_upload_log(tmp_path):
    from encode_file_transfer import s3Helper
    s3h = s3Helper()
    from encode_file_transfer.interface import LOGBUCKET
    make_bucket(LOGBUCKET)
    locallog = tmp_path / 'locallog.txt'
    locallog.write_text(u'content')
    assert s3h._upload_log(str(locallog))


@mock_s3
def test_s3_helper_upload_file_metadata(tmp_path):
    from encode_file_transfer import s3Helper
    s3h = s3Helper()
    from encode_file_transfer.interface import PUBLIC_BUCKET
    make_bucket(PUBLIC_BUCKET)
    localmanifest = tmp_path / 'localmanifest.txt'
    localmanifest.write_text(u'content')
    assert s3h._upload_file_metadata(str(localmanifest))
