from dataclasses import dataclass
import logging
from urllib.parse import urlparse
from typing import Annotated

import rich.traceback
from rich.progress import Progress
import structlog
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

LOG: structlog.stdlib.BoundLogger = structlog.get_logger()


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
    """Ensures there's no leading or trailing /"""
    path = path if not path.startswith("/") else path[1:]
    return path if not path.endswith("/") else path[:-1]


@app.command()
def compare(
    source: Annotated[S3Prefix, typer.Argument(parser=parse_prefix)],
    path: str,
    dest: Annotated[S3Prefix, typer.Argument(parser=parse_prefix)],
):
    path = make_path_relative(path)
    comparison = compare_buckets(source, dest, path)
    LOG.info(
        "comparison",
        common=list(comparison.common),
        different=list(comparison.different),
        only_in_destination=list(comparison.only_in_destination),
        only_in_source=list(comparison.only_in_source),
    )


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
    comparison = compare_buckets(source, dest, path)

    downloader = S3Downloader
    uploader = S3Uploader

    if download_only:
        downloader = S3Downloader
        uploader = DryRunUploader
    if dry_run:
        downloader = DryRunDownloader
        uploader = DryRunUploader

    with Progress() as progress:
        syncs_task = progress.add_task("Syncs", total=len(comparison.to_sync))
        for key in comparison.to_sync:
            result = sync_key(source, dest, key, downloader, uploader)
            LOG.info("sync.result", result=result)
            progress.update(syncs_task, advance=1)


@app.callback()
def main(
    verbose: bool = True,
    debug: bool = False,
    quiet: bool = False,
    use_json_logging: bool = False,
) -> None:
    rich.traceback.install(show_locals=True)
    log_level = logging.INFO if verbose else logging.WARNING
    log_level = logging.DEBUG if debug else log_level
    log_level = logging.WARNING if quiet else log_level
    if use_json_logging:
        # Configure same processor stack as default, minus dev bits
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.StackInfoRenderer(),
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.processors.JSONRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(log_level),
        )
    else:
        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(log_level),
        )


if __name__ == "__main__":
    app()
