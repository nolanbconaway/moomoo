"""Utility functions for the good of all."""

import datetime
import hashlib
import os
from itertools import groupby
from pathlib import Path
from typing import Iterable, Iterator

import musicbrainzngs
import requests
import tenacity

SPECIAL_PURPOSE_ARTISTS = {
    "f731ccc4-e22a-43af-a747-64213329e088",  # anonymous
    "33cf029c-63b0-41a0-9855-be2a3665fb3b",  # data
    "314e1c25-dde7-4e4d-b2f4-0a7b9f7c56dc",  # dialogue
    "eec63d3c-3b81-4ad4-b1e4-7c147d4d2b61",  # no artist
    "9be7f096-97ec-4615-8957-8d40b5dcbc41",  # traditional
    "125ec42a-7229-4250-afc5-e057484327fe",  # unknown
    "89ad4ac3-39f7-470e-963a-56509c546377",  # various artists
}


def moomoo_version() -> str:
    """Get the version of this package."""
    return (Path(__file__).resolve().parent / "version").read_text().strip()


# set user agent for all musicbrainzngs requests
musicbrainzngs.set_useragent(
    app="moomoo-ingest",
    version="moomoo_version()",
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
    # if more than 25 releases, fetch all releases via browse. see limitation here:
    # https://python-musicbrainzngs.readthedocs.io/en/v0.7.1/usage/?highlight=browse#regular-musicbrainz-data
    release_count = int(data["artist"].get("release-count", 0))

    # no need to walk large release lists for special purpose artists
    if release_count > 25 and artist_mbid not in SPECIAL_PURPOSE_ARTISTS:
        release_list = []  # data['artist']['release-list']
        limit = 25
        offsets = range(0, release_count, limit)
        for offset in offsets:
            releases = musicbrainzngs.browse_releases(
                artist=artist_mbid, includes=[], limit=limit, offset=offset
            )
            release_list += releases["release-list"]

        # deduplicate the release list in case a release was added during the fetches
        release_list = [
            next(iter(releases))
            for _, releases in groupby(
                sorted(release_list, key=lambda x: x["id"]), key=lambda x: x["id"]
            )
        ]

        # reassign to expected location
        data["artist"]["release-list"] = release_list

    return data


ENTITIES = ["recording", "release", "artist", "release-group"]


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

    try:
        return dict(_success=True, _args=args, data=fn(mbid))
    except Exception as e:
        return dict(_success=False, _args=args, error=str(e))


def annotate_mbid_batch(mbids_maps: Iterable[dict]) -> Iterator[dict]:
    """Enrich MusicBrainz IDs with data from MusicBrainz.

    Expected input is a list/iterable of dicts with the following keys:

    - mbid: the MusicBrainz ID
    - entity: the type of entity, e.g. 'recording', 'release', 'artist'

    Yields a generator of dicts with the following keys:

    - _success: boolean indicating whether the request was successful
    - _args: a dict containing the mbid and entity type of the request
    - error: error message if the request was not successful
    - data: the data returned from MusicBrainz if the request was successful
    """
    for mbid_map in mbids_maps:
        mbid = mbid_map["mbid"]
        entity = mbid_map["entity"]
        yield annotate_mbid(mbid, entity)


def batch(iterable, n=1) -> Iterator[Iterable]:
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx : min(ndx + n, length)]


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
