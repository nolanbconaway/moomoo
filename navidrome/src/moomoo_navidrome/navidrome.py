"""Module for managing Navidrome resource management.

Handles auth, path resolution, etc via Subsonic API. See API docs here
https://opensubsonic.netlify.app/docs/
"""

import contextlib
import hashlib
import os
import secrets
import sqlite3
import time
from collections.abc import Generator
from pathlib import Path
from typing import TypedDict

import httpx

from moomoo_navidrome.logger import logger
from moomoo_navidrome.models import NavidromePlaylist, SubsonicResponse, SubsonicStatusError
from moomoo_navidrome.utils_ import batched


class SubsonicAuth(TypedDict):
    """Represents the authentication parameters required for Subsonic API calls.

    Attributes:
        u: The username.
        t: The MD5 token (password + salt).
        s: The random salt used for the token.
        v: The API version.
        c: The client identifier.
        f: The requested response format (json/xml).
    """

    u: str
    t: str
    s: str
    v: str
    c: str
    f: str


class NavidromeHTTPClient(httpx.Client):
    """An httpx.Client subclass pre-configured for Navidrome/Subsonic.

    Every request made through this client automatically includes the
    required Subsonic authentication parameters in the query string.
    """

    def __init__(self, **kwargs):
        """Initializes the client using environment variables for auth and host.

        Args:
            **kwargs: Passed to httpx.Client (e.g., timeout, limits).
        """
        # Fetch configuration from environment
        host = os.environ["NAVIDROME_URL"]
        username = os.environ["NAVIDROME_USERNAME"]
        password = os.environ["NAVIDROME_PASSWORD"]

        # Generate Subsonic-specific salt and token
        salt = secrets.token_hex(6)
        token = hashlib.md5((password + salt).encode()).hexdigest()

        self.subsonic_params: SubsonicAuth = {
            "u": username,
            "t": token,
            "s": salt,
            "v": "1.16.1",
            "c": "moomoo",
            "f": "json",
        }

        # Apply moomoo defaults if not explicitly overridden
        kwargs.setdefault("base_url", host)
        kwargs.setdefault("timeout", 60.0)

        # Add the hook to the client configuration
        kwargs["event_hooks"] = {"response": [self.auto_raise_on_status_or_subsonic_error]}

        # Merge subsonic_params with any user-provided params
        initial_params = kwargs.pop("params", {})
        kwargs["params"] = {**initial_params, **self.subsonic_params}

        super().__init__(**kwargs)

    def auto_raise_on_status_or_subsonic_error(self, response: httpx.Response):
        """Intercepts response to check for Subsonic application errors.

        Automatically raises an httpx.HTTPStatusError for non-2xx HTTP responses, and a
        SubsonicStatusError for any 200 OK response where the Subsonic 'status' field is 'failed'.
        """
        response.raise_for_status()
        # Only process successful HTTP responses that contain JSON
        if "application/json" in response.headers.get("Content-Type", ""):
            response.read()
            envelope = SubsonicResponse(**response.json().get("subsonic-response", {}))
            if envelope.status == "failed":
                raise SubsonicStatusError(
                    f"Subsonic Error {envelope.error.code}: {envelope.error.message}",
                    request=response.request,
                    response=response,
                    code=envelope.error.code,
                )

    def fetch_playlists(self) -> list[NavidromePlaylist]:
        """Fetches all playlists from Navidrome and validates them via Pydantic v2.

        Returns:
            list[NavidromePlaylist]: List of validated playlist models.
        """
        resp = self.get("/rest/getPlaylists")
        data = resp.json().get("subsonic-response", {}).get("playlists", {}).get("playlist", [])

        # Note: Subsonic returns a dict if count == 1, or a list if count > 1.
        if isinstance(data, dict):
            data = [data]

        return [NavidromePlaylist(**p) for p in data]

    def get_playlist_by_id(self, playlist_id: str) -> NavidromePlaylist | None:
        """Fetches a single playlist by ID and validates it via Pydantic v2.

        Args:
            playlist_id: The ID of the playlist to fetch.

        Returns:
            NavidromePlaylist: The playlist model if found, else None.
        """
        try:
            resp = self.get("/rest/getPlaylist", params={"id": playlist_id})
        except SubsonicStatusError as e:
            if e.subsonic_code == 70:  # Not Found
                return None
            raise
        data = resp.json().get("subsonic-response", {}).get("playlist")
        if not data:
            return None
        return NavidromePlaylist(**data)

    def delete_playlist(
        self,
        playlist_id: str,
        timeout_seconds: int = 10,
        interval: float = 0.5,
    ) -> bool:
        """Delete a playlist and poll until it is confirmed gone or timeout is reached."""
        # delete the playlist
        self.get("/rest/deletePlaylist", params={"id": playlist_id})

        # poll for confirmation
        start_time = time.monotonic()
        while (time.monotonic() - start_time) < timeout_seconds:
            playlist = self.get_playlist_by_id(playlist_id)
            if playlist is None:
                return True
            time.sleep(interval)

        # 3. If we break the loop, it timed out
        raise TimeoutError(
            f"Playlist {playlist_id} was not deleted after {timeout_seconds} seconds."
        )

    def add_songs_to_playlist(
        self, playlist_id: str, song_ids: list[str], chunk_size: int = 50
    ) -> None:
        """Add tracks in sequential chunks to preserve order and avoid URL limits."""
        for chunk in batched(song_ids, chunk_size):
            # OpenSubsonic /updatePlaylist uses 'songIdToAdd' to append
            # Passing a list to 'params' makes httpx repeat the key: ?songIdToAdd=A&songIdToAdd=B
            self.get(
                "/rest/updatePlaylist",
                params={"playlistId": playlist_id, "songIdToAdd": list(chunk)},
            )

    def create_playlist(self, name: str, comment: str, song_ids: list[str]) -> str:
        """Create a new playlist with the given name, comment, and songs.

        Args:
            name: The name of the playlist.
            comment: A comment/description for the playlist.
            song_ids: A list of song IDs to include in the playlist.

        Returns:
            str: The ID of the newly created playlist.
        """
        # Create the playlist with just the name first
        create_resp = self.get("/rest/createPlaylist", params={"name": name})
        pl_id: str = create_resp.json()["subsonic-response"]["playlist"]["id"]

        try:
            # add the comment
            self.post(
                "/rest/updatePlaylist", params={"playlistId": pl_id}, data={"comment": comment}
            )

            # add the songs in chunks to preserve order
            if song_ids:
                self.add_songs_to_playlist(pl_id, song_ids)
                time.sleep(0.5)

            # check that the comment was added correctly, if not then raise an error.
            #
            # if the comment is not added correctly, then we have no way to manage the playlist
            # and will end up with duplicates on the next sync.
            check_plist = self.get_playlist_by_id(pl_id)
            if check_plist.comment != comment:
                raise RuntimeError("Comment was not added correctly.")

        except Exception:
            logger.exception(f"Failed to create playlist '{name}', rolling back.")
            self.delete_playlist(pl_id)
            raise

        return pl_id


class NavidromeDBClient:
    """Handles direct read-only SQLite interactions with the Navidrome database.

    Attributes:
        db_path: The filesystem path to the navidrome.db file.
    """

    def __init__(self, db_path: Path | str | None = None, readonly: bool = True) -> None:
        """Initializes the DB client.

        Args:
            db_path: Path to the navidrome.db SQLite file. Defaults to the NAVIDROME_DB_PATH
            environment variable if not provided.
        """
        if db_path is None:
            db_path = os.environ["NAVIDROME_DB_PATH"]
        self.db_path = Path(db_path)

        # mode=ro ensures we cannot accidentally modify the production DB
        uri = f"file:{self.db_path.absolute()}"
        if readonly:
            uri += "?mode=ro"
        else:
            # confirm that navidrome is offline before allowing a writable connection
            self.check_navidrome_offline()
        self.uri = uri

    @staticmethod
    def check_navidrome_offline() -> None:
        """Check that Navidrome is not running.

        Raises:
            RuntimeError: If Navidrome is still running.
        """
        url = os.environ["NAVIDROME_URL"]
        try:
            with httpx.Client(timeout=5.0) as client:
                response = client.get(url, follow_redirects=True)
                response.raise_for_status()
            raise RuntimeError(
                f"Navidrome is still running at {url} (status {response.status_code}). "
                "Shut it down before modifying the database."
            )
        except (httpx.HTTPStatusError, httpx.RequestError):
            pass

    @contextlib.contextmanager
    def connect(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for a read-only SQLite connection."""
        conn = sqlite3.connect(self.uri, uri=True)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def resolve_paths_to_ids(
        self, file_paths: list[Path], chunk_size: int = 100
    ) -> dict[Path, str]:
        """Maps file paths to internal Navidrome IDs via direct SQLite access.

        Args:
            file_paths: A list of file paths relative to the music root.
                        Assumes moomoo and Navidrome share the same root.

        Returns:
            dict[Path, str]: A mapping of {original_path_object: navidrome_id}.

        Raises:
            sqlite3.Error: If there is a failure accessing the database.
        """
        if not file_paths:
            return {}

        # Pre-map strings to Path objects for fast lookup after query
        path_lookup: dict[str, Path] = {p.as_posix(): p for p in file_paths}
        path_strs: list[str] = list(path_lookup.keys())
        mapping: dict[Path, str] = {}

        with self.connect() as conn:
            cursor = conn.cursor()
            for chunk in batched(path_strs, chunk_size):
                placeholders = ",".join(["?"] * len(chunk))

                query = f"SELECT path, id FROM media_file WHERE path IN ({placeholders})"
                cursor.execute(query, chunk)

                for row_path, row_id in cursor.fetchall():
                    if row_path in path_lookup:
                        mapping[path_lookup[row_path]] = row_id

        return mapping

    def get_song_ids(self, file_paths: list[Path]) -> list[str]:
        """Convenience method to get just the list of song IDs for given file paths.

        This does so in an ordered fashion, skipping any paths that don't resolve to an ID.
        """
        path_to_id = self.resolve_paths_to_ids(file_paths)
        return [path_to_id[p] for p in file_paths if p in path_to_id]

    def list_loved_files(self) -> set[Path]:
        """Lists the file paths of all loved tracks in Navidrome."""
        sql = """
        select distinct media_file.path
        from annotation
        inner join media_file
            on annotation.item_id = media_file.id
        where annotation.starred = 1
        and annotation.item_type = 'media_file'
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return {Path(row["path"]) for row in cursor.fetchall()}
