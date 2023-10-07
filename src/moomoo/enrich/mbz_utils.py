"""Musicbrainz utilties."""
import os
from typing import Iterable, Iterator

import musicbrainzngs

from .. import utils_

# set user agent for all musicbrainzngs requests
musicbrainzngs.set_useragent(
    app="moomoo",
    version=utils_.moomoo_version(),
    contact=os.environ.get("MOOMOO_CONTACT_EMAIL"),
)


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


def _get_release_data(release_mbid: str) -> dict:
    """Get release data from MusicBrainz."""
    return musicbrainzngs.get_release_by_id(
        release_mbid,
        includes=[
            "artists",
            "labels",
            "recordings",
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
    return musicbrainzngs.get_artist_by_id(
        artist_mbid,
        includes=[
            "releases",
            "various-artists",
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


def annotate_mbid(mbid: str, entity: str) -> dict:
    """Enrich a MusicBrainz IDs with data from MusicBrainz.

    Expected input:

    - mbid: the MusicBrainz ID
    - entity: the type of entity, e.g. 'recording', 'release', 'artist'

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
