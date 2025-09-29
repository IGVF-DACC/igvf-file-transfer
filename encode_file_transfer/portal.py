import logging
import requests
from requests.exceptions import ConnectionError
from urllib.parse import (
    urlparse,
    urljoin,
    urlunsplit,
    urlencode,
)
from .interface import (
    AUDIT,
    AUDIT_TYPE,
    AUDIT_CATEGORY,
    INDEXER,
    SPLITQUERYTEMPLATE,
    FILE_METADATA_QUERY_TEMPLATE,
    FILE_METADATA_FIELDS,
    FILE_METADATA_STATUSES,
    FILE_METADATA_UPLOAD_STATUSES,
)


log = logging.getLogger()


class EncodePortalHelper():

    def __init__(self, server, **kwargs):
        self.server = server
        self.creds = kwargs.get('portal_creds')
        self.batch_size = kwargs.get('batch_size')
        self.query_filter = kwargs.get('query_filter')
        self.file_metadata_fields = kwargs.get('fields', FILE_METADATA_FIELDS)
        self.file_metadata_statuses = kwargs.get('statuses', FILE_METADATA_STATUSES)
        self.file_metadata_upload_statuses = kwargs.get('upload_statuses', FILE_METADATA_UPLOAD_STATUSES)

    @staticmethod
    def _zero_search_results(r):
        if r.status_code != 404:
            return False
        try:
            r = r.json()
        except Exception as e:
            r = {}
        conditions = [
            '@graph' in r,
            'total' in r,
            'notification' in r,
            len(r.get('@graph', [])) == 0,
            r.get('total') == 0,
            r.get('notification') == 'No results found',
        ]
        return all(conditions)

    def _get(self, url, creds=None):
        log.warning('Getting {}'.format(url))
        try:
            r = requests.get(url, auth=creds or self.creds)
        except ConnectionError as e:
            log.warning('URL not found. Does {} exist?'.format(url))
            raise e
        if r.status_code != 200 and not self._zero_search_results(r):
            log.warning('Status code not 200. Does {} exist?'.format(url))
            log.warning('{} {}'.format(r.status_code, r.text))
            raise ValueError('Bad response code')
        return r

    def _patch(self, url, json, creds=None):
        log.warning('Patching {} with {}'.format(url, json))
        try:
            r = requests.patch(url, json=json, auth=creds or self.creds)
        except ConnectionError as e:
            log.warning('URL not found. Does {} exist?'.format(url))
            raise e
        if r.status_code != 200:
            log.warning('Status code not 200. Does {} exist?'.format(url))
            log.warning('{} {}'.format(r.status_code, r.text))
            raise ValueError('Bad response code')
        return r

    @staticmethod
    def _parse_query_filter(query_filter):
        return urlencode(query_filter, doseq=True)

    def _make_audit_query(self, batch_size=None, query_filter=None):
        split_query = SPLITQUERYTEMPLATE.copy()
        split_query[1] = urlparse(self.server).netloc
        if batch_size:
            split_query[3] += '&limit={}'.format(batch_size)
        if query_filter:
            split_query[3] += '&{}'.format(self._parse_query_filter(query_filter))
        return urlunsplit(tuple(split_query))

    def _parse_audits(self, query_results):
        '''
        Returns tuple (accession, incorrect file bucket details).
        '''
        # Must use @id for replaced and reference files.
        internal_audits = [
            (
                q.get('@id'),
                q.get(AUDIT, {}).get(AUDIT_TYPE, [])
            )
            for q in query_results
        ]
        parsed_audits = []
        for f in internal_audits:
            accession = f[0]
            for audit in f[1]:
                if audit.get('category') == AUDIT_CATEGORY:
                    parsed_audits.append((accession, audit.get('detail')))
        return parsed_audits

    def _make_metadata_query(self):
        metadata_query = FILE_METADATA_QUERY_TEMPLATE.copy()
        metadata_query[1] = urlparse(self.server).netloc
        batch_size = self.batch_size
        if batch_size:
            metadata_query[3] += '&limit={}'.format(batch_size)
        metadata_query[3] += '&{}'.format(
            urlencode(
                {
                    'field': self.file_metadata_fields,
                    'status': self.file_metadata_statuses,
                    'upload_status': self.file_metadata_upload_statuses,
                    'controlled_access!': 'true',
                    'externally_hosted!': 'true',
                    'audit.INTERNAL_ACTION.category!': 'incorrect file bucket',
                },
                doseq=True
            )
        )
        return urlunsplit(tuple(metadata_query))

    def _flatten_list(self, values):
        if isinstance(values, list):
            for value in values:
                yield from self._flatten_list(value)
        else:
            yield values

    def _flatten_json(self, data):
        flattened_data = {}
        for field in self.file_metadata_fields:
            path = field.split('.')
            v = data
            for p in path:
                if isinstance(v, list):
                    v = list(self._flatten_list([x.get(p) for x in v]))
                else:
                    v = v.get(p)
                if not v:
                    break
            flattened_data[field] = v
        return flattened_data

    def _parse_metadata(self, metadata):
        parsed_metadata = []
        for data in metadata:
            parsed_metadata.append(
                self._flatten_json(
                    data
                )
            )
        return parsed_metadata

    def get_file_metadata(self):
        file_metadata_query = self._make_metadata_query()
        r = self._get(file_metadata_query)
        parsed_metadata = self._parse_metadata(r.json().get('@graph', []))
        log.warning('Got {} files for metadata'.format(len(parsed_metadata)))
        return parsed_metadata

    def is_indexing(self):
        r = self._get(urljoin(self.server, INDEXER))
        return r.json().get('is_indexing') is True

    def get_files_in_incorrect_bucket(self):
        file_audit_query = self._make_audit_query(self.batch_size, self.query_filter)
        r = self._get(file_audit_query)
        parsed_audits = self._parse_audits(r.json().get('@graph', []))
        return parsed_audits
