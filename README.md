## Move files between private and public s3 buckets and keep ENCODE portal metadata in sync.

In general the script can be run independently by switching to this repo and running:

```bash
$ pip install -e .
$ pip install -r requirements.txt
```

Then starting Python and initializing `EncodeFileTransfer` with portal and AWS credentials:
```python
portal_creds = ('xxx', 'yyy')
aws_creds = ('mmm', 'zzz')
from encode_file_transfer import EncodeFileTransfer
server = 'https://www.encodeproject.org'
eft = EncodeFileTransfer(
    server,
    batch_size='all',
    portal_creds=portal_creds,
    aws_creds=aws_creds,
    query_filter={
        'status': 'released',
        'output_category': 'raw data',
        'no_file_available': 'false',
        'restricted!': '*'
    }
)
eft.sync_buckets_and_portal()
```

This will pull files from the portal that have an incorrect bucket audit and match the specified `query_filter` (optional). You can also specify an integer for the `batch_size` to limit the number of files retrieved.

Here are a few examples of internal methods that can be useful.

Get files to move:

```python
>>> from encode_file_transfer import EncodeFileTransfer
>>> server = 'https://encd-4427-file-bucket-audit-07aae6e4f-keenan.demo.encodedcc.org'
>>> eft = EncodeFileTransfer(server, batch_size=5)
>>> files_to_move = eft._get_files_to_move()
>>> pd.DataFrame(files_to_move)
     accession source_bucket                                         source_key     destination_bucket                                    destination_key
0  ENCFF321OXI  encode-files  2019/02/09/dc1388a0-7a81-4255-8de1-a1bd186208f...  encode-pds-public-dev  2019/02/09/dc1388a0-7a81-4255-8de1-a1bd186208f...
1  ENCFF758RFO  encode-files  2019/02/09/653cadee-5227-4d22-9aa2-e9945069617...  encode-pds-public-dev  2019/02/09/653cadee-5227-4d22-9aa2-e9945069617...
2  ENCFF129XEJ  encode-files  2019/02/09/f65faec7-ee4d-4672-8b9e-bc954ce3af1...  encode-pds-public-dev  2019/02/09/f65faec7-ee4d-4672-8b9e-bc954ce3af1...
3  ENCFF158VOT  encode-files  2019/02/09/ca0cdc07-f58a-4143-bd69-71d1a8b237b...  encode-pds-public-dev  2019/02/09/ca0cdc07-f58a-4143-bd69-71d1a8b237b...
4  ENCFF812XZP  encode-files  2019/02/09/534be4c1-5771-44aa-a01f-fae4ca910b9...  encode-pds-public-dev  2019/02/09/534be4c1-5771-44aa-a01f-fae4ca910b9...
```

Get only in progress files (using portal_creds and query_filter):

```python
>>> from encode_file_transfer import EncodeFileTransfer
>>> portal_creds = ('xxx', 'yyy')
>>> server = 'https://encd-4427-file-bucket-audit-07aae6e4f-keenan.demo.encodedcc.org'
>>> eft = EncodeFileTransfer(server, batch_size=5, portal_creds=portal_creds, query_filter={'status': 'in progress'})
>>> files_to_move = eft._get_files_to_move()
>>> pd.DataFrame(files_to_move)
     accession source_bucket                                         source_key      destination_bucket                                    destination_key
0  ENCFF077CVI  encode-files  2019/02/11/fb0cc28e-ddb9-44c6-a597-69f71a75d6a...  encode-pds-private-dev  2019/02/11/fb0cc28e-ddb9-44c6-a597-69f71a75d6a...
1  ENCFF768LTV  encode-files  2019/02/11/2bdca263-ddeb-410a-a978-4143e4e08dc...  encode-pds-private-dev  2019/02/11/2bdca263-ddeb-410a-a978-4143e4e08dc...
2  ENCFF910LZK  encode-files  2019/02/11/0e18d1ba-4804-4ca8-9e8e-65eb640b990...  encode-pds-private-dev  2019/02/11/0e18d1ba-4804-4ca8-9e8e-65eb640b990...
3  ENCFF841WLM  encode-files  2019/02/11/7eb5631e-e8a6-457a-8c7a-7bd94511cea...  encode-pds-private-dev  2019/02/11/7eb5631e-e8a6-457a-8c7a-7bd94511cea...
4  ENCFF350UKY  encode-files  2019/02/11/d43aad73-c251-4b3f-9238-a6bea8d2b66...  encode-pds-private-dev  2019/02/11/d43aad73-c251-4b3f-9238-a6bea8d2b66...
```

Pass in AWS and portal creds, move first released file, and patch portal:

```python
>>> from encode_file_transfer import EncodeFileTransfer
>>> aws_creds = ('mmm', 'zzz')
>>> portal_creds = ('xxx', 'yyy')
>>> server = 'https://encd-4455-bucket-notify-eff7f3c1a-keenan.demo.encodedcc.org'
>>> eft = EncodeFileTransfer(server, batch_size=5, portal_creds=portal_creds, aws_creds=aws_creds, query_filter={'status': 'released'})
>>> files_to_move = eft._get_files_to_move()
>>> f = files_to_move[0]
>>> f
{'destination_key': '2019/02/11/f3036636-383a-465b-af6b-17ebc9a3ebb0/ENCFF915PJP.bigBed', 'accession': 'ENCFF915PJP', 'destination_bucket': 'encode-pds-public-dev', 'source_key': '2019/02/11/f3036636-383a-465b-af6b-17ebc9a3ebb0/ENCFF915PJP.bigBed', 'source_bucket': 'encode-files'}
>>> eft.s3h._move_file(f)
Copying encode-files/2019/02/11/f3036636-383a-465b-af6b-17ebc9a3ebb0/ENCFF915PJP.bigBed to encode-pds-public-dev/2019/02/11/f3036636-383a-465b-af6b-17ebc9a3ebb0/ENCFF915PJP.bigBed
True
>>> eft._update_bucket_on_portal(f)
<Response [200]>
```

Dump file metadata to S3 bucket:

```python
>>> aws_creds = ('mmm', 'zzz')
>>> portal_creds = ('xxx', 'yyy')
>>> from encode_file_transfer import EncodeFileTransfer
>>> server = 'https://www.encodeproject.org'
>>> eft = EncodeFileTransfer(server, batch_size='all', portal_creds=portal_creds, aws_creds=aws_creds)
>>> eft.dump_file_metadata_to_s3()
```

## Create Docker container and run on AWS batch

In order to run the script automatically we can build a Docker container with the script inside, push it to [Amazon ECR](https://us-west-2.console.aws.amazon.com/ecr/repositories/encode-file-transfer/?region=us-west-2), and schedule a batch job to run periodically:

```bash
# Using pds-test-user AWS creds
$ AWS_ACCESS_KEY_ID=ABC AWS_SECRET_ACCESS_KEY=XYZ aws ecr get-login --no-include-email
# Enter returned Docker credentials from above.
$ docker build -t encode-file-transfer .
$ docker tag encode-file-transfer:latest 220748714863.dkr.ecr.us-west-2.amazonaws.com/encode-file-transfer:latest
$ docker push 220748714863.dkr.ecr.us-west-2.amazonaws.com/encode-file-transfer:latest
```

This should be run everytime the code is updated.

The Docker container will by default sync files:

```bash
$ docker run encode-file-transfer sync

```

But it can also be used to dump metadata:

```bash
docker run encode-file-transfer metadata
```

However the container must be run in an environment with AWS credential that can access the parameter store in the public account (which is why it is easiest to run it on AWS Batch compute).

Note that the file sync is scheduled to run every night at 11:59 PCT and the metadata dump at 1:59 PCT.

## Tests

Tests can be run with pytest.
