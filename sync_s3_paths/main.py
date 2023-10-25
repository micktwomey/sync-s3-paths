import asyncio
import logging
from dataclasses import dataclass
from typing import Annotated
from urllib.parse import urlparse

import rich.traceback
import psutil
import structlog
import typer
from rich.progress import Progress

from .sync import (
    Comparison,
    Downloader,
    DryRunDownloader,
    DryRunUploader,
    Key,
    S3Downloader,
    S3Prefix,
    S3Uploader,
    Uploader,
    compare_buckets,
    sync_key,
    SyncResult,
    S3Client,
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


@dataclass
class SyncEvent:
    source: S3Prefix
    dest: S3Prefix
    key: str
    downloader: Downloader
    uploader: Uploader


async def sync_worker(
    name: str,
    inbox: asyncio.Queue[SyncEvent],
    outbox: asyncio.Queue[SyncResult],
    is_finished: asyncio.Event,
):
    s3_clients: dict[S3Prefix, S3Client] = {}
    while True:
        try:
            event = await asyncio.wait_for(inbox.get(), 1.0)
        except asyncio.TimeoutError:
            if is_finished.is_set():
                return
            continue
        LOG.debug("worker.event-start", name=name, sync_event=event)
        if event.source not in s3_clients:
            s3_clients[event.source] = event.source.get_s3_client()
        if event.dest not in s3_clients:
            s3_clients[event.dest] = event.dest.get_s3_client()
        try:
            result = await asyncio.to_thread(
                sync_key,
                source=event.source,
                source_s3_client=s3_clients[event.source],
                dest=event.dest,
                dest_s3_client=s3_clients[event.dest],
                key=event.key,
                downloader=event.downloader,
                uploader=event.uploader,
            )
        except Exception as e:
            LOG.exception("error", worker=name, sync_event=event)
            result = SyncResult(
                error=str(e),
                success=False,
                bucket=event.dest.bucket,
                dest=event.dest,
                source=event.source,
                etag=None,
                key=event.key,
                md5=None,
                sha256=None,
                size=0,
            )
        await outbox.put(result)
        inbox.task_done()
        LOG.debug("worker.event-done", name=name, sync_event=event, result=result)


async def do_sync(
    source: S3Prefix,
    dest: S3Prefix,
    comparison: Comparison,
    uploader: Uploader,
    downloader: Downloader,
):
    outbox: asyncio.Queue[SyncEvent] = asyncio.Queue()
    inbox: asyncio.Queue[SyncResult] = asyncio.Queue()
    is_finished = asyncio.Event()
    workers = []
    for i in range(psutil.cpu_count() * 2):
        worker = asyncio.create_task(
            sync_worker(
                name=f"sync-worker-{i}",
                inbox=outbox,
                outbox=inbox,
                is_finished=is_finished,
            )
        )
        workers.append(worker)
    errors = 0
    successes = 0
    with Progress() as progress:
        tasks_remaining = len(comparison.to_sync)
        syncs_task = progress.add_task("Syncs", total=tasks_remaining)

        for key in comparison.to_sync:
            await outbox.put(
                SyncEvent(
                    source=source,
                    dest=dest,
                    key=key,
                    downloader=downloader,
                    uploader=uploader,
                )
            )

        running = True
        while running:
            try:
                result = await asyncio.wait_for(inbox.get(), 1.0)
                LOG.info("sync.result", result=result)
                progress.update(syncs_task, advance=1)
                if result.success:
                    successes += 1
                else:
                    errors += 1
                    LOG.error("error", error=result.error)
                tasks_remaining -= 1
            except asyncio.TimeoutError:
                # Check if we've processed all events
                if (
                    (inbox.qsize() == 0)
                    and (outbox.qsize() == 0)
                    and (tasks_remaining == 0)
                ):
                    is_finished.set()
                    await outbox.join()
                    for worker in workers:
                        worker.cancel()
                    for result in await asyncio.gather(
                        *workers, return_exceptions=True
                    ):
                        if isinstance(result, Exception):
                            LOG.error("shutdown.exception", exception=result)
                    running = False
    assert inbox.qsize() == 0, inbox.qsize()
    assert outbox.qsize() == 0, outbox.qsize()
    LOG.info(
        "synced",
        synced=successes,
        errors=errors,
        only_in_destination=len(comparison.only_in_destination),
        identical=len(comparison.common),
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
    LOG.info("comparing-buckets", source=source, dest=dest, path=path)
    comparison = compare_buckets(source, dest, path)

    downloader = S3Downloader
    uploader = S3Uploader

    if download_only:
        downloader = S3Downloader
        uploader = DryRunUploader
    if dry_run:
        downloader = DryRunDownloader
        uploader = DryRunUploader

    LOG.info("syncing")
    asyncio.run(
        do_sync(
            source=source,
            dest=dest,
            comparison=comparison,
            uploader=uploader,
            downloader=downloader,
        )
    )


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
