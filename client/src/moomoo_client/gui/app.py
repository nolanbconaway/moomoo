import os

import toga
from toga.sources.list_source import Row as TogaRow
from toga.style import Pack

from ..http import PlaylistRequester
from ..logger import logger
from ..utils_ import VERSION, Playlist

USERNAME = os.environ["LISTENBRAINZ_USERNAME"]
logger.info("listenbrainz_username", username=USERNAME)


class PlaylistTable(toga.Table):
    """Container for the playlists."""

    def __init__(self, id: str):
        super().__init__(
            id=id,
            data=[],
            headings=["id", "description"],
            multiple_select=False,
            on_activate=self.activate_playlist_handler,
            style=Pack(
                flex=1,
                padding_right=5,
                font_family="monospace",
            ),
        )

    def activate_playlist_handler(self, _: toga.Widget, row: TogaRow, **__) -> None:
        """Handle the selection of a playlist."""
        playlist: Playlist = row.playlist
        logger.info(
            "activate_playlist",
            description=playlist.description,
            playlist_id=playlist.playlist_id,
        )
        playlist.to_strawberry()

    def add_playlist(self, playlist: Playlist):
        """Add a playlist."""
        self.data.append(
            {
                "id": playlist.playlist_id.hex[:8],
                "description": playlist.description,
                "playlist": playlist,  # keep a reference to the playlist
            }
        )


class MoomooApp(toga.App):
    def startup(self):
        """Build the frame of the app.

        This needs to be very fast as it is run on the main thread prior to first paint.
        No http, etc.
        """
        self.main_window = toga.MainWindow(title=self.formal_name, size=(800, 600))
        main_box = toga.Box(style=Pack(direction="column", padding=10))
        main_box.add(
            toga.Label(
                text="Welcome to Moomoo GUI!",
                style=Pack(font_family="sans-serif", font_size=20),
            )
        )

        main_box.add(toga.Divider(style=Pack(padding_top=10, padding_bottom=20)))
        main_box.add(
            toga.Label(
                text="Double click a playlist to open it in Strawberry.",
                style=Pack(font_family="sans-serif", font_size=12),
            )
        )

        playlists = toga.ScrollContainer(
            horizontal=False,
            content=PlaylistTable(id="playlists_list"),
            style=Pack(padding_top=10, height=400),
        )
        main_box.add(playlists)

        main_box.add(toga.Divider(style=Pack(padding_top=10, padding_bottom=20)))
        main_box.add(
            toga.Label(
                text=f"v{VERSION}",
                style=Pack(font_family="sans-serif", font_size=8),
            )
        )

        self.main_window.content = main_box
        self.main_window.show()

    async def populate_artist_playlists(self, app: "MoomooApp"):
        logger.info("populate_artist_playlists")
        playlists_list: PlaylistTable = app.widgets["playlists_list"]
        requester = PlaylistRequester()
        playlists = await requester.request_user_artist_suggestions(USERNAME, 10)
        for playlist in playlists:
            playlists_list.add_playlist(playlist)


def create_app() -> MoomooApp:
    logger.info("create_app")
    app = MoomooApp("moomoo gui", app_id="com.moomoo.ui")
    app.add_background_task(app.populate_artist_playlists)
    return app


def main():
    app = create_app()
    app.main_loop()


if __name__ == "__main__":
    main()
