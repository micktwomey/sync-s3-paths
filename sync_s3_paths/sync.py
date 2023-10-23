import datetime
from dataclasses import dataclass
from io import BufferedReader, BytesIO, BufferedIOBase
from typing import Callable, Iterable, TypedDict
import hashlib
import tempfile
import base64

import boto3
import botocore.response
from rich import print
import psutil


@dataclass
class Key:
    key: str
    size: int
    etag: str
    last_modified: datetime.datetime


@dataclass
class S3Prefix:
    profile: str | None
    bucket: str
    prefix: str | None

    def get_session(self) -> boto3.Session:
        return boto3.Session(profile_name=self.profile)

    def get_key(self, key: str) -> str:
        return f"{self.prefix}/{key}"


@dataclass
class Comparison:
    only_in_source: set[str]
    only_in_destination: set[str]
    different: set[str]

    @property
    def to_sync(self):
        return self.only_in_source.union(self.different)


@dataclass
class SyncResult:
    source: S3Prefix
    dest: S3Prefix
    bucket: str
    key: str
    size: int
    etag: str | None
    sha256: str
    md5: str


@dataclass
class DownloadResult:
    bucket: str
    key: str
    size: int
    content_type: str
    metadata: dict[str, str]
    etag: str | None
    data: botocore.response.StreamingBody | BufferedReader


Downloader = Callable[[S3Prefix, str], DownloadResult]


@dataclass
class UploadResult:
    bucket: str
    key: str
    size: int
    etag: str | None
    sha256: str
    md5: str


Uploader = Callable[[S3Prefix, str, DownloadResult], UploadResult]


def DryRunDownloader(prefix: S3Prefix, key: str) -> DownloadResult:
    return DownloadResult(
        bucket=prefix.bucket,
        key=prefix.get_key(key),
        size=0,
        content_type="application/binary",
        metadata={},
        etag=None,
        data=BytesIO(),
    )


class S3GetObjectResponse(TypedDict):
    Body: botocore.response.StreamingBody
    ContentLength: int
    ContentType: str
    ETag: str
    Metadata: dict[str, str]


def S3Downloader(prefix: S3Prefix, key: str) -> DownloadResult:
    session = prefix.get_session()
    s3 = session.client("s3")
    response: S3GetObjectResponse = s3.get_object(
        Bucket=prefix.bucket,
        Key=prefix.get_key(key),
    )
    print(response)
    return DownloadResult(
        bucket=prefix.bucket,
        key=prefix.get_key(key),
        size=response["ContentLength"],
        content_type=response["ContentType"],
        metadata=response["Metadata"],
        etag=response["ETag"],
        data=response["Body"],
    )


def DryRunUploader(
    prefix: S3Prefix, key: str, downloaded: DownloadResult
) -> UploadResult:
    sha256 = hashlib.sha256()
    md5 = hashlib.md5()
    while True:
        chunk = downloaded.data.read(1024)
        if chunk == b"":
            break
        sha256.update(chunk)
        md5.update(chunk)
    return UploadResult(
        bucket=prefix.bucket,
        key=prefix.get_key(key),
        size=downloaded.size,
        etag=None,
        sha256=sha256.hexdigest(),
        md5=md5.hexdigest(),
    )


class S3PutObjectResponse(TypedDict):
    ETag: str
    ChecksumSHA256: str


def S3Uploader(prefix: S3Prefix, key: str, downloaded: DownloadResult) -> UploadResult:
    session = prefix.get_session()
    s3 = session.client("s3")
    # We can't stream directly from a get object to a put object, boto3 tries to read the object to
    # calculate the MD5, then seek() back. To mitigate this try to read ourselves, spooling to disk
    # as needed.
    # It's a shame you can't upload, get the checksum back and compare after the fact.
    sha256 = hashlib.sha256()
    md5 = hashlib.md5()

    # TODO: make this tunable but for now limit ourselves to 5% or 1MiB of memory max
    available_memory = max(int(psutil.virtual_memory().available * 0.05), 1024**2)

    buffer: BufferedIOBase
    with tempfile.TemporaryFile() as fp:
        if downloaded.size <= available_memory:
            print(f"Storing {key} in memory")
            buffer = BytesIO()
        else:
            buffer = fp

        while True:
            chunk = downloaded.data.read(1024)
            if chunk == b"":
                break

            sha256.update(chunk)
            md5.update(chunk)
            buffer.write(chunk)

        buffer.seek(0)

        kwargs = dict(
            Bucket=prefix.bucket,
            Body=buffer,
            ContentLength=downloaded.size,
            ContentType=downloaded.content_type,
            Key=prefix.get_key(key),
            Metadata=downloaded.metadata,
            ChecksumSHA256=base64.b64encode(sha256.digest()).decode("ascii"),
            ChecksumAlgorithm="SHA256",
            ContentMD5=base64.b64encode(md5.digest()).decode("ascii"),
        )
        print(kwargs)
        response: S3PutObjectResponse = s3.put_object(**kwargs)
        print(response)
        return UploadResult(
            bucket=prefix.bucket,
            size=downloaded.size,
            key=prefix.get_key(key),
            etag=response["ETag"],
            sha256=sha256.hexdigest(),
            md5=md5.hexdigest(),
        )


def list_keys_in_prefix(session: boto3.Session, prefix: S3Prefix) -> Iterable[Key]:
    s3 = session.client("s3")

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=prefix.bucket, Delimiter="", Prefix=prefix.prefix
    ):
        if "Contents" not in page:
            continue
        for key in page["Contents"]:
            key_key = key["Key"]
            assert key_key.startswith(prefix.prefix)
            if prefix.prefix is not None:
                key_key = key_key[len(prefix.prefix) :]
                key_key = key_key[1:] if key_key.startswith("/") else key_key
                assert not key_key.startswith("/")
            yield Key(
                key=key_key,
                size=key["Size"],
                etag=key["ETag"],
                last_modified=key["LastModified"],
            )


def compare_buckets(source: S3Prefix, dest: S3Prefix) -> Comparison:
    source_session = source.get_session()
    dest_session = dest.get_session()

    source_keys: dict[str, Key] = {}
    for key in list_keys_in_prefix(source_session, source):
        source_keys[key.key] = key

    dest_keys: dict[str, Key] = {}
    for key in list_keys_in_prefix(dest_session, dest):
        dest_keys[key.key] = key

    only_in_source: set[str] = set(source_keys).difference(set(dest_keys))
    only_in_dest: set[str] = set(dest_keys).difference(set(source_keys))
    different: set[str] = set()
    for key_key in set(source_keys).intersection(set(dest_keys)):
        source_key = source_keys[key_key]
        dest_key = dest_keys[key_key]
        if (source_key.size != dest_key.size) or (
            source_key.last_modified > dest_key.last_modified
        ):
            different.add(key_key)

        # TODO: Compare etags, need to account for differences due to encryption at rest and multi part upload
        # See https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#checking-object-integrity-etag-and-md5
        # See also aws s3 sync (which appears to use size and last modifed): https://github.com/aws/aws-cli/blob/develop/awscli/customizations/s3/syncstrategy/base.py

    return Comparison(
        only_in_source=only_in_source,
        only_in_destination=only_in_dest,
        different=different,
    )


def sync_key(
    source: S3Prefix,
    dest: S3Prefix,
    key: str,
    downloader: Downloader,
    uploader: Uploader,
) -> SyncResult:
    downloaded = downloader(source, key)
    print(downloaded)
    uploaded = uploader(dest, key, downloaded)
    print(uploaded)
    return SyncResult(
        source=source,
        dest=dest,
        bucket=uploaded.bucket,
        key=uploaded.key,
        size=uploaded.size,
        etag=uploaded.etag,
        sha256=uploaded.sha256,
        md5=uploaded.md5,
    )
