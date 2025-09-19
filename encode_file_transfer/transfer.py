import boto3
import logging
import time
from datetime import datetime
from botocore.exceptions import ClientError
from urllib.parse import (
    urlparse,
    urljoin,
)
import pandas as pd
from .interface import (
    PORTAL_CREDS,
    BUCKET_UPDATE,
    ORIGINAL_BUCKET,
    PUBLIC_BUCKET,
    BATCH_SIZE,
    LOGFILE,
    METADATA_TSV,
    GLACIER_TAG_SET
)
from .portal import EncodePortalHelper


def logger(filename):
    log = logging.getLogger()
    log.setLevel(logging.WARN)
    return log


LOGFILE = LOGFILE.format(datetime.today().strftime('%Y-%m-%d-%Hh-%Mm-%Ss'))
log = logger(LOGFILE)


class s3Helper():

    def __init__(self, original_bucket=ORIGINAL_BUCKET, **kwargs):
        self.original_bucket = original_bucket
        self.awsid, self.awspw = kwargs.get('aws_creds', (None, None))

    @staticmethod
    def _parse_file_to_move(file_to_move):
        return (
            file_to_move.get(x)
            for x in [
                    'source_bucket',
                    'source_key',
                    'destination_bucket',
                    'destination_key'
            ]
        )

    def _get_session(self):
        session = boto3.Session(
            aws_access_key_id=self.awsid,
            aws_secret_access_key=self.awspw,
        )
        return session

    def _file_exists(self, bucket, key):
        '''
        Check to see if bucket/key exists.
        '''
        session = self._get_session()
        s3 = session.resource('s3')
        try:
            s3.Object(bucket, key).load()
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise e
        return True

    def _move_file(self, file_to_move):
        '''
        Move file from source bucket to destination bucket.
        '''
        sb, sk, db, dk = self._parse_file_to_move(file_to_move)
        if sb == db and sk == dk:
            log.warning('Source and destination same. Skipping move!')
            return True
        session = self._get_session()
        s3 = session.resource('s3')
        source = {
            'Bucket': sb,
            'Key': sk,
        }
        log.warning('Copying {}/{} to {}/{}'.format(sb, sk, db, dk))
        s3.meta.client.copy(source, db, dk)
        return True

    def _delete_file(self, file_to_move):
        '''
        Delete file from source bucket.
        '''
        sb, sk, db, dk = self._parse_file_to_move(file_to_move)
        if sb == self.original_bucket:
            log.warning('Not deleting from {}'.format(self.original_bucket))
            return False
        if sb == db and sk == dk:
            log.warning('Source and destination same. Skipping delete!')
            return False
        log.warning('Deleting {}/{}'.format(sb, sk))
        session = self._get_session()
        s3 = session.resource('s3')
        s3.Object(sb, sk).delete()
        return True

    def _tag_file(self, file_to_move):
        '''
        Maybe tag s3 object to be moved to glacier storage. 
        '''
        sb, sk, db, dk = self._parse_file_to_move(file_to_move)
        if sb != self.original_bucket:
            log.warning('Object not in {}. Skipping tag!'.format(self.original_bucket))
            return False
        session = self._get_session()
        s3 = session.resource('s3')
        log.warning(
            'Tagging {}/{} with: {}'.format(
                sb,
                sk,
                GLACIER_TAG_SET
            )
        )
        s3.meta.client.put_object_tagging(
            Bucket=sb,
            Key=sk,
            Tagging=GLACIER_TAG_SET
        )
        return True

    def _upload_file_metadata(self, localmanifest=METADATA_TSV):
        log.warning('Uploading file manifest {} to s3'.format(localmanifest))
        session = self._get_session()
        s3 = session.resource('s3')
        s3.meta.client.upload_file(
            localmanifest,
            PUBLIC_BUCKET,
            METADATA_TSV,
            ExtraArgs={'ACL': 'bucket-owner-full-control'}
        )
        return True


class EncodeFileTransfer():

    def __init__(self, server, original_bucket=ORIGINAL_BUCKET, initial_transfer=False, **kwargs):
        '''
        Initial_transfer is boolean flag to handle the full sync we did
        between encode-files and encode-public. It shouldn't be used
        after the initial portal patch, because we always want to overwrite
        destination object with source object in case of corner case of
        reupload of file that has already been moved out of encode-files.
        (We should also never use the raw sync command again after we start
        patching, as it will indiscriminately move all files into
        encode-public.)
        
        With the initial_transfer flag set to True we will search for file in
        encode-public before transfering:
            1. If it exists in encode-public then encode-public will get set as the source_bucket.
            2. If it doesn't exist in encode-public then the parsed source_bucket will remain the same.
        Transfer step will only actually transfer something if source_bucket != destination bucket.
        '''
        self.original_bucket = original_bucket
        self.server = server
        self.eph = EncodePortalHelper(server, **kwargs)
        self.s3h = s3Helper(**kwargs)
        self.files_to_move = None
        self.initial_transfer = initial_transfer

    @staticmethod
    def _parse_s3_to_bucket_and_key(s3_uri):
        parsed_s3uri = urlparse(s3_uri)
        bucket = parsed_s3uri.netloc
        # Strip leading /.
        key = parsed_s3uri.path[1:]
        return {'Bucket': bucket, 'Key': key}

    def _parse_audit_details_for_source_and_destination(self, parsed_audits):
        accession, audit_detail = parsed_audits
        status = audit_detail.split('Move')[1].split(' ')[1]
        audit_split = audit_detail.split(' to ')
        source_s3_uri_parsed = self._parse_s3_to_bucket_and_key(
            audit_split[0].split(' from ')[-1]
        )
        destination_s3_uri_parsed = self._parse_s3_to_bucket_and_key(
            audit_split[-1]
        )
        return {
            'accession': accession,
            'status': status,
            'source_bucket': source_s3_uri_parsed.get('Bucket'),
            'source_key': source_s3_uri_parsed.get('Key'),
            'destination_bucket': destination_s3_uri_parsed.get('Bucket'),
            'destination_key': destination_s3_uri_parsed.get('Key')
        }

    def _get_files_to_move(self):
        '''
        Returns list of objects to move.
        '''
        files_to_move = []
        parsed_audits = self.eph.get_files_in_incorrect_bucket()
        for p in parsed_audits:
            files_to_move.append(
                self._parse_audit_details_for_source_and_destination(p)
            )
        log.warning('Got {} files to move'.format(len(files_to_move)))
        return files_to_move

    def _make_bucket_update_url(self, accession):
        bucket_update = BUCKET_UPDATE
        return urljoin(self.server, bucket_update.format(accession).strip('/'))

    def _update_bucket_on_portal(self, f):
        '''
        Patches bucket location of file on portal server.
        '''
        url = self._make_bucket_update_url(f.get('accession'))
        r = self.eph._patch(
            url,
            {'new_bucket': f.get('destination_bucket')}
        )
        return r

    def _wait_for_indexer(self, times=[0, 120, 240]):
        '''
        Only wait for indexer for so long before giving up.
        '''
        for t in times:
            time.sleep(t)
            if not self.eph.is_indexing():
                log.warning('Portal waiting, continuing')
                return True
            log.warning('Portal indexing, retrying soon')
        log.warning('Portal still indexing, exiting')
        return False

    def _set_source_to_encode_public(self, f):
        _, sk, _, _ = self.s3h._parse_file_to_move(f)
        if self.s3h._file_exists(PUBLIC_BUCKET, sk):
            f['source_bucket'] = PUBLIC_BUCKET
        return f

    def _determine_source(self, f):
        '''
        Pick up failures if they have occured.
        '''
        sb, sk, db, dk = self.s3h._parse_file_to_move(f)
        # Most cases should be transfer from source to destination, i.e.
        # either transfer hasn't happend yet or transfer happened but
        # delete failed.
        if self.s3h._file_exists(sb, sk):
            pass
        # Transfer and delete happened but portal failed to patch.
        elif self.s3h._file_exists(db, dk):
            f['source_bucket'] = db
            f['source_key'] = dk
        # File doesn't exist in source or destination. Try to copy from
        # original bucket.
        elif self.s3h._file_exists(self.original_bucket, sk):
            f['source_bucket'] = self.original_bucket
        else:
            log.warning(
                '{} does not exist in any bucket! Skipping!'.format(f['accession'])
            )
            return False
        return f

    def _make_metadata_tsv(self, parsed_metadata, filename):
        log.warning('Dumping metadata to {}'.format(filename))
        df = pd.DataFrame(parsed_metadata)
        df = df.sort_values(
            by=[
                'file_set.accession',
                'assembly',
                'file_format'
            ]
        ).reset_index(drop=True)
        df[self.eph.file_metadata_fields].to_csv(filename, sep='\t', index=False)

    def dump_file_metadata_to_s3(self):
        if not self._wait_for_indexer():
            return False
        parsed_metadata = self.eph.get_file_metadata()
        self._make_metadata_tsv(
            parsed_metadata,
            METADATA_TSV
        )
        self.s3h._upload_file_metadata()

    def sync_buckets_and_portal(self):
        '''
        Pull files with incorrect bucket audit.
        '''
        if not self._wait_for_indexer():
            return False
        files_to_move = self._get_files_to_move()
        try:
            for i, f in enumerate(files_to_move):
                log.warning(
                    '\n{}\t{}\t{}\t{}'.format(
                        i,
                        f['accession'],
                        f['status'],
                        datetime.now()
                    )
                )
                if self.initial_transfer:
                    # Try to set source to encode-public.
                    f = self._set_source_to_encode_public(f)
                # Check for previous incomplete transfers.
                f = self._determine_source(f)
                # The file doesn't exist in any bucket, so skip and clean up audit later.
                if not f:
                    continue
                # Move file to destination.
                self.s3h._move_file(f)
                # Tag original file for glacier storage.
                self.s3h._tag_file(f)
                # Delete file from source.
                self.s3h._delete_file(f)
                # Patch portal with new bucket.
                self._update_bucket_on_portal(f)
        except Exception as e:
            log.exception('Exception on {}'.format(f))
            raise
        finally:
            print('Done')
        return True
