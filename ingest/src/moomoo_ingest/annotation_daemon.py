import datetime
import time

import click

from .annotate_mbids import fetch_from_queue, ingest_batch

ANY_DATA_SLEEP_SECONDS = 10  # sleep this many seconds between batches if data found
NO_DATA_SLEEP_SECONDS = 60 * 5  # sleep this many seconds if no data found
REPORT_INTERVAL_ITEMS = 25  # report every N items annotated


def log_with_timestamp(message: str, *args, **kwargs):
    """Log a message with a timestamp."""
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    click.echo(f"[{timestamp}] {message}", *args, **kwargs)


def run(new_: bool, updated: bool, reannotate_after_days: int, batch_size: int) -> int:
    """Run a batch of annotations."""
    log_with_timestamp(f"Starting annotation of batch of size {batch_size}.")

    reannotate_ts = (
        None
        if reannotate_after_days <= 0
        else (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(days=reannotate_after_days)
        )
    )

    batch = fetch_from_queue(
        new_=new_,
        updated=updated,
        reannotate_ts=reannotate_ts,
        batch_size=batch_size,
        loggerfn=log_with_timestamp,
    )
    n = ingest_batch(batch=batch, loggerfn=log_with_timestamp)
    log_with_timestamp("Completed annotation batch.")
    return n


@click.command(help=__doc__)
@click.option(
    "--new",
    "new_",
    is_flag=True,
    default=True,
    help="Option to detect new mbids that have not been annotated yet.",
)
@click.option(
    "--updated",
    "updated",
    is_flag=True,
    default=True,
    help="Option to detect mbids that have been updated since they were last annotated.",
)
@click.option(
    "--reanntotate-after-days",
    "reannotate_after_days",
    type=int,
    default=180,
    help="Option to detect mbids that were annotated more than OLD_DAYS ago for re-annotation.",
)
@click.option(
    "--batch",
    "batch_size",
    type=int,
    default=200,
)
def main(new_: bool, updated: bool, reannotate_after_days: int, batch_size: int) -> int:
    """Run the main CLI."""
    try:
        while True:
            n = run(
                new_=new_,
                updated=updated,
                reannotate_after_days=reannotate_after_days,
                batch_size=batch_size,
            )
            if n > 0:
                log_with_timestamp(f"Annotated {n} items, sleeping {ANY_DATA_SLEEP_SECONDS}s.")
                time.sleep(ANY_DATA_SLEEP_SECONDS)
            else:
                log_with_timestamp(f"Nothig done, sleeping {NO_DATA_SLEEP_SECONDS}s.")
                time.sleep(NO_DATA_SLEEP_SECONDS)

    except Exception as e:
        log_with_timestamp(f"Fatal error, exiting: {e}", err=True)
        raise
    finally:
        log_with_timestamp("Service stopped")


if __name__ == "__main__":
    main()
