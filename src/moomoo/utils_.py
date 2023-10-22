"""Utility functions for the good of all.

Put no specialty imports beyond cli, postgres here, as the thin client needs this.
"""
import datetime
import json
from pathlib import Path
from uuid import UUID


class UUIDEncoder(json.JSONEncoder):
    """JSON encoder for UUIDs."""

    def default(self, obj):
        """Encode UUIDs as hex strings."""
        if isinstance(obj, UUID):
            return obj.hex
        return json.JSONEncoder.default(self, obj)


def moomoo_version() -> str:
    """Get the current moomoo version."""
    return (Path(__file__).resolve().parent / "version").read_text().strip()


def utcfromisodate(iso_date: str) -> datetime.datetime:
    """Convert YYYY-MM-DD date string to UTC datetime."""
    dt = datetime.datetime.fromisoformat(iso_date)
    if dt.tzinfo is not None:
        return dt.astimezone(datetime.timezone.utc)
    return dt.replace(tzinfo=datetime.timezone.utc)


def utcfromunixtime(unixtime: int) -> datetime.datetime:
    """Convert unix timestamp to UTC datetime."""
    return datetime.datetime.utcfromtimestamp(int(unixtime)).replace(
        tzinfo=datetime.timezone.utc
    )


def utcnow() -> datetime.datetime:
    """Get the current UTC datetime."""
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
