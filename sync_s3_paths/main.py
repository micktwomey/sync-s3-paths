from dataclasses import dataclass
from urllib.parse import urlparse
from typing import Annotated

from rich import print
import typer

from .sync import (
    S3Prefix,
    compare_buckets,
    Key,
    sync_key,
    DryRunDownloader,
    DryRunUploader,
    S3Downloader,
    S3Uploader,
)

app = typer.Typer()


@dataclass
class SyncAction:
    source: S3Prefix
    dest: S3Prefix
    key: Key


def parse_prefix(prefix: str) -> S3Prefix:
    parts = urlparse(prefix)
    assert parts.scheme == "s3"
    assert parts.hostname is not None
    path = (
        parts.path[1:]
        if (parts.path is not None and parts.path.startswith("/"))
        else parts.path
    )
    return S3Prefix(profile=parts.username, bucket=parts.hostname, prefix=path)


def make_path_relative(path: str) -> str:
    """Ensures there's no leading /"""
    return path if not path.startswith("/") else path[1:]


@app.command()
def compare(
    source: Annotated[S3Prefix, typer.Argument(parser=parse_prefix)],
    path: str,
    dest: Annotated[S3Prefix, typer.Argument(parser=parse_prefix)],
):
    path = make_path_relative(path)
    comparison = compare_buckets(source, dest)
    print(comparison)


@app.command()
def sync(
    source: Annotated[S3Prefix, typer.Argument(parser=parse_prefix)],
    path: str,
    dest: Annotated[S3Prefix, typer.Argument(parser=parse_prefix)],
    dry_run: Annotated[
        bool, typer.Option(help="Simulate download and upload of objects")
    ] = False,
    download_only: Annotated[
        bool,
        typer.Option(help="Download from source s3 but simulate upload to target S3"),
    ] = False,
):
    path = make_path_relative(path)
    comparison = compare_buckets(source, dest)

    downloader = S3Downloader
    uploader = S3Uploader

    if dry_run:
        downloader = DryRunDownloader
        uploader = DryRunUploader
    if download_only:
        downloader = S3Downloader
        uploader = DryRunUploader

    for key in comparison.to_sync:
        result = sync_key(source, dest, key, downloader, uploader)
        print(result)


if __name__ == "__main__":
    app()
