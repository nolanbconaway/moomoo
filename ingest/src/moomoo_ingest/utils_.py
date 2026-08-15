"""Utility functions for the good of all."""

import asyncio
import datetime
import functools
import hashlib
import os
import random
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from itertools import groupby
from pathlib import Path

import musicbrainzngs
import requests
import tenacity
from liblistenbrainz import ListenBrainz

SPECIAL_PURPOSE_ARTISTS = {
    "f731ccc4-e22a-43af-a747-64213329e088",  # anonymous
    "33cf029c-63b0-41a0-9855-be2a3665fb3b",  # data
    "314e1c25-dde7-4e4d-b2f4-0a7b9f7c56dc",  # dialogue
    "eec63d3c-3b81-4ad4-b1e4-7c147d4d2b61",  # no artist
    "9be7f096-97ec-4615-8957-8d40b5dcbc41",  # traditional
    "125ec42a-7229-4250-afc5-e057484327fe",  # unknown
    "fdcc79ef-0832-4c23-a4f9-5c5a4160083d",  # unknown
    "89ad4ac3-39f7-470e-963a-56509c546377",  # various artists
}

# timeout applied to annotation functions, which may consist of dozens of requests internally
MUSICBRAINZ_TIMEOUT = 5 * 60
EXECUTOR = ThreadPoolExecutor(max_workers=1)


def moomoo_version() -> str:
    """Get the version of this package."""
    return (Path(__file__).resolve().parent / "version").read_text().strip()


# set user agent for all musicbrainzngs requests
musicbrainzngs.set_useragent(
    app="moomoo-ingest",
    version=moomoo_version(),
    contact=os.environ.get("MOOMOO_CONTACT_EMAIL"),
)


def utcfromisodate(iso_date: str) -> datetime.datetime:
    """Convert YYYY-MM-DD date string to UTC datetime."""
    dt = datetime.datetime.fromisoformat(iso_date)
    if dt.tzinfo is not None:
        return dt.astimezone(datetime.timezone.utc)
    return dt.replace(tzinfo=datetime.timezone.utc)


def utcfromunixtime(unixtime: int) -> datetime.datetime:
    """Convert unix timestamp to UTC datetime."""
    return datetime.datetime.fromtimestamp(int(unixtime), tz=datetime.timezone.utc)


def utcnow() -> datetime.datetime:
    """Get the current UTC datetime."""
    return datetime.datetime.now(datetime.timezone.utc)


def md5(*args: str) -> str:
    """Get the md5 hash of the given strings."""
    return hashlib.md5("-".join(args).encode()).hexdigest()


def get_listenbrainz_client() -> ListenBrainz:
    """Get a ListenBrainz client.

    Sets the auth token from the LISTENBRAINZ_USER_TOKEN environment variable. Extracted here
    also to facilitate mocking in tests.
    """
    client = ListenBrainz()
    client.set_auth_token(os.environ.get("LISTENBRAINZ_USER_TOKEN"), check_validity=False)
    return client


class MusicBrainzTimeoutError(Exception):
    """Custom exception for MusicBrainz timeouts."""


class MusicBrainzResponseError(Exception):
    """Custom exception for MusicBrainz response errors."""


def clean_exception_wrapper(fn):
    """Decorator to clean up exceptions raised by MusicBrainz functions.

    Musicbrainz response errors contain urllib httperrors as causes which are not pickleable, which
    breaks serialization. So we catch them and raise a custom exception instead.
    """

    @functools.wraps(fn)
    def wrapper(mbid):
        try:
            return fn(mbid)
        except musicbrainzngs.ResponseError as e:
            status = getattr(e.cause, "code", None)  # should be an httperror with a code attr.
            message = e.message or getattr(e.cause, "msg", None)
            raise MusicBrainzResponseError(
                f"MusicBrainz error for {mbid}: status={status}, message={message}, cause={e.cause}"
            ) from None  # do not allow http error context to propagate

    return wrapper


@clean_exception_wrapper
def _get_recording_data(recording_mbid: str) -> dict:
    """Get release data from MusicBrainz."""
    return musicbrainzngs.get_recording_by_id(
        recording_mbid,
        includes=[
            "artists",
            "releases",
            "artist-credits",
            "aliases",
            "tags",
            "ratings",
            "area-rels",
            "artist-rels",
            "label-rels",
            "place-rels",
            "url-rels",
        ],
    )


@clean_exception_wrapper
def _get_release_group_data(release_group_mbid: str) -> dict:
    """Get release group data from MusicBrainz."""
    return musicbrainzngs.get_release_group_by_id(
        release_group_mbid,
        includes=[
            "artists",
            "releases",
            "aliases",
            "tags",
            "area-rels",
            "artist-rels",
            "label-rels",
            "place-rels",
            "event-rels",
            "recording-rels",
            "release-rels",
            "release-group-rels",
            "series-rels",
            "url-rels",
        ],
    )


@clean_exception_wrapper
def _get_release_data(release_mbid: str) -> dict:
    """Get release data from MusicBrainz."""
    return musicbrainzngs.get_release_by_id(
        release_mbid,
        includes=[
            "artists",
            "labels",
            "recordings",
            "release-groups",
            "aliases",
            "tags",
            "area-rels",
            "artist-rels",
            "label-rels",
            "place-rels",
            "url-rels",
        ],
    )


@clean_exception_wrapper
def _get_artist_data(artist_mbid: str) -> dict:
    """Get artist data from MusicBrainz."""
    data = musicbrainzngs.get_artist_by_id(
        artist_mbid,
        includes=[
            "releases",
            "aliases",
            "area-rels",
            "artist-rels",
            "label-rels",
            "place-rels",
            "url-rels",
            "tags",
            "ratings",
        ],
    )
    # browese releases to get the full release list, since the get_artist_by_id call only returns up
    # to 25 releases.
    release_count = int(data["artist"].get("release-count", 0))

    # no need to walk large release lists for special purpose artists
    if artist_mbid not in SPECIAL_PURPOSE_ARTISTS:
        release_list = []  # data['artist']['release-list']

        # musicbrainz may return fewer than the limit, due to the 500 track handler. So we need
        # to keep track of the number of releases in each request rather than assume we get 25
        # each time.
        #
        # see https://musicbrainz.org/doc/MusicBrainz_API
        current_count, limit = 0, 25
        while True:
            releases = musicbrainzngs.browse_releases(
                artist=artist_mbid,
                includes=[],
                limit=limit,
                offset=current_count,
            )
            batch = releases.get("release-list", [])
            current_count += len(batch)
            release_list += batch

            if not batch or current_count >= release_count:
                break

        # deduplicate the release list in case a release was added during the fetches
        release_list = list(unique_by(release_list, key=lambda x: x["id"]))

        # reassign to expected location
        data["artist"]["release-list"] = release_list
    else:
        data["artist"]["release-list"] = []

    return data


ENTITIES = ["recording", "release", "artist", "release-group"]


async def _annotate_mbid_async(mbid: str, entity: str) -> dict:
    """Async base for annotate_mbid.

    Figures out which sync function to call based on entity type, then runs it in an executor with
    a timeout. Raises MusicBrainzTimeoutError on timeout.
    """

    # check contact email set
    if not os.environ.get("MOOMOO_CONTACT_EMAIL"):
        raise ValueError("MOOMOO_CONTACT_EMAIL environment variable not set.")

    args = dict(mbid=mbid, entity=entity)
    fn = {
        "recording": _get_recording_data,
        "release": _get_release_data,
        "artist": _get_artist_data,
        "release-group": _get_release_group_data,
    }.get(entity)

    if fn is None:
        return dict(_success=False, _args=args, error=f"Unknown entity type: {entity}.")

    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(EXECUTOR, fn, mbid)
    try:
        data = await asyncio.wait_for(task, timeout=MUSICBRAINZ_TIMEOUT)
        return dict(_success=True, _args=args, data=data)
    except asyncio.TimeoutError as e:
        raise MusicBrainzTimeoutError from e
    except Exception as e:
        return dict(_success=False, _args=args, error=str(e))


def annotate_mbid(mbid: str, entity: str) -> dict:
    """Enrich a MusicBrainz IDs with data from MusicBrainz.

    Expected input:

    - mbid: the MusicBrainz ID
    - entity: the type of entity: 'recording', 'release', 'artist', 'release-group'

    Returns a dicts with the following keys:

    - _success: boolean indicating whether the request was successful
    - _args: a dict containing the mbid and entity type of the request
    - error: error message if the request was not successful
    - data: the data returned from MusicBrainz if the request was successful

    Raises MusicBrainzTimeoutError on timeout.
    """
    return asyncio.run(_annotate_mbid_async(mbid, entity))


def batch(iterable, n=1) -> Iterator[Iterable]:
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx : min(ndx + n, length)]


def unique_by(items: Iterable, key: Callable) -> Iterator[dict]:
    """Yield unique items from an iterable of dicts based on a specified key."""
    for _, group in groupby(sorted(items, key=key), key=key):
        yield next(iter(group))


def topn_from_multilists(
    lists: list[list], N: int, identity_fn: Callable, shuffle: bool = True
) -> list:
    """Deduplicate and select N items from lists of lists.

    Selects up to N unique items from lists of lists based on a specified identity func.
    Grabs items from the input lists in order, ensuring no duplicates based on the identity.

    Args:
        lists: A list of lists containing the items from which to select.
        N: The maximum number of unique items to select.
        identity_fn: A callable that takes an item and returns its unique identity.
        shuffle: Whether to shuffle each input list before selecting items.
    """
    seen = set()
    output = []

    for lst in lists:
        if shuffle:
            random.shuffle(lst)
        for item in lst:
            k = identity_fn(item)
            if k not in seen:
                seen.add(k)
                output.append(item)
                if len(output) == N:
                    return output

    return output


@tenacity.retry(
    retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, max=60),
    reraise=True,
)
def request_with_retry(method: str, url: str, timeout: int = 30, **kwargs) -> requests.Response:
    """Simple request wrapper with retries."""
    resp = requests.request(method=method, url=url, timeout=timeout, **kwargs)
    resp.raise_for_status()
    return resp
