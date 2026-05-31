import os

from liblistenbrainz import ListenBrainz


def get_listenbrainz_client() -> ListenBrainz:
    """Get a ListenBrainz client.

    Sets the auth token from the LISTENBRAINZ_USER_TOKEN environment variable. Extracted here
    also to facilitate mocking in tests.
    """
    client = ListenBrainz()
    client.set_auth_token(os.environ.get("LISTENBRAINZ_USER_TOKEN"), check_validity=False)
    return client
