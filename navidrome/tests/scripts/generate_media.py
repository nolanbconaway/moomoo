"""Create minimal FLAC audio files for test fixtures.

Writes a small but realistic library structure to tests/resources/music/.
"""

import io
import struct
from dataclasses import dataclass
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPTS_DIR = Path(__file__).parent
TESTS_DIR = SCRIPTS_DIR.parent
MUSIC_DIR = TESTS_DIR / "resources" / "music"


# ---------------------------------------------------------------------------
# Minimal FLAC builder
# ---------------------------------------------------------------------------


def _encode_utf8_int(n: int) -> bytes:
    """Encode an integer as a UTF-8-style coded number (used in FLAC frame headers)."""
    if n < 0x80:
        return bytes([n])
    elif n < 0x800:
        return bytes([0xC0 | (n >> 6), 0x80 | (n & 0x3F)])
    else:
        return bytes([0xE0 | (n >> 12), 0x80 | ((n >> 6) & 0x3F), 0x80 | (n & 0x3F)])


def _crc8(data: bytes) -> int:
    """CRC-8 with poly 0x07, used in FLAC frame headers."""
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = ((crc << 1) ^ 0x07) & 0xFF if crc & 0x80 else (crc << 1) & 0xFF
    return crc


def _crc16(data: bytes) -> int:
    """CRC-16 with poly 0x8005, used in FLAC frame footers."""
    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x8005) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def _make_vorbis_comment_block(tags: dict[str, str], is_last: bool = False) -> bytes:
    """
    Build a FLAC VORBIS_COMMENT metadata block (block type 4).
    This is how FLAC stores title/artist/album/etc.
    """

    def encode_field(key: str, value: str) -> bytes:
        s = f"{key}={value}".encode()
        return struct.pack("<I", len(s)) + s

    vendor = b"test-fixture-generator"
    fields = [encode_field(k, v) for k, v in tags.items()]

    body = io.BytesIO()
    body.write(struct.pack("<I", len(vendor)))
    body.write(vendor)
    body.write(struct.pack("<I", len(fields)))
    for field in fields:
        body.write(field)

    payload = body.getvalue()
    block_type = (0x80 if is_last else 0x00) | 4  # type 4 = VORBIS_COMMENT
    return bytes([block_type]) + len(payload).to_bytes(3, "big") + payload


def make_silent_flac(
    *,
    title: str,
    artist: str,
    album: str,
    track_number: int,
    track_total: int,
    year: int = 2024,
    genre: str = "Test",
    sample_rate: int = 44100,
    channels: int = 2,
    bits_per_sample: int = 16,
    num_samples: int = 4410,  # 0.1 seconds
) -> bytes:
    """
    Build a minimal but structurally valid FLAC file with Vorbis Comment tags.

    The audio content is a single constant (silent) frame — just enough for
    Navidrome's scanner to accept and index the file. The file is ~500 bytes.
    """
    out = io.BytesIO()

    # ---- fLaC marker -------------------------------------------------------
    out.write(b"fLaC")

    # ---- STREAMINFO block (type 0) -----------------------------------------
    si = io.BytesIO()
    min_block_size = 4096
    max_block_size = 4096
    si.write(struct.pack(">HH", min_block_size, max_block_size))
    si.write(b"\x00\x00\x00")  # min frame size (unknown)
    si.write(b"\x00\x00\x00")  # max frame size (unknown)
    # 20-bit sample rate | 3-bit (channels-1) | 5-bit (bps-1) | 36-bit total samples
    packed = (
        (sample_rate & 0xFFFFF) << 44
        | ((channels - 1) & 0x7) << 41
        | ((bits_per_sample - 1) & 0x1F) << 36
        | (num_samples & 0xFFFFFFFFF)
    )
    si.write(packed.to_bytes(8, "big"))
    si.write(b"\x00" * 16)  # MD5 (unknown)
    streaminfo_payload = si.getvalue()

    # Not the last block — VORBIS_COMMENT follows
    out.write(bytes([0x00]))
    out.write(len(streaminfo_payload).to_bytes(3, "big"))
    out.write(streaminfo_payload)

    # ---- VORBIS_COMMENT block (type 4, last block) -------------------------
    tags = {
        "TITLE": title,
        "ARTIST": artist,
        "ALBUMARTIST": artist,
        "ALBUM": album,
        "TRACKNUMBER": f"{track_number}/{track_total}",
        "DATE": str(year),
        "GENRE": genre,
    }
    out.write(_make_vorbis_comment_block(tags, is_last=True))

    # ---- Audio frame -------------------------------------------------------
    # A single constant (silent) FLAC frame.
    # Frame header fields:
    #   sync (14 bits) = 0x3FFE
    #   reserved (1 bit) = 0
    #   blocking strategy (1 bit) = 0 (fixed)  → bytes 0xFF 0xF8
    #   block size in header (4 bits): 0001 = 192 samples
    #   sample rate in header (4 bits): 1001 = 44.1 kHz
    #   channel assignment (4 bits): 0001 = stereo
    #   sample size (3 bits): 100 = 16 bits per sample
    #   reserved (1 bit) = 0
    frame_header_before_crc = bytes(
        [
            0xFF,
            0xF8,  # sync + blocking strategy
            0x19,  # block size=192 | sample rate=44100
            0x14,  # channels=stereo | bps=16 | reserved
        ]
    )
    frame_number = _encode_utf8_int(0)
    header_for_crc = frame_header_before_crc + frame_number
    crc8 = _crc8(header_for_crc)
    frame_header = header_for_crc + bytes([crc8])

    # SUBFRAME_CONSTANT: type bits 0b000000, wasted bits = 0 → byte 0x00
    # constant value: 0 (16 bits = 2 bytes)
    subframe = bytes([0x00, 0x00, 0x00])  # subframe type + 16-bit zero sample

    frame_body = frame_header + subframe
    crc16 = _crc16(frame_body)
    frame = frame_body + struct.pack(">H", crc16)

    out.write(frame)

    return out.getvalue()


# ---------------------------------------------------------------------------
# Library definition
# ---------------------------------------------------------------------------


@dataclass
class TrackSpec:
    path: str  # relative to MUSIC_DIR
    title: str
    artist: str
    album: str
    track_number: int
    track_total: int
    year: int = 2024
    genre: str = "Test"


LIBRARY: list[TrackSpec] = [
    # Artist Alpha — Album One
    TrackSpec(
        "Artist Alpha - Album One/01 - First Track.flac",
        "First Track",
        "Artist Alpha",
        "Album One",
        1,
        2,
    ),
    TrackSpec(
        "Artist Alpha - Album One/02 - Second Track.flac",
        "Second Track",
        "Artist Alpha",
        "Album One",
        2,
        2,
    ),
    # Artist Alpha — Album Two
    TrackSpec(
        "Artist Alpha - Album Two/01 - Only Track.flac",
        "Only Track",
        "Artist Alpha",
        "Album Two",
        1,
        1,
    ),
    # Artist Beta — Album Three
    TrackSpec(
        "Artist Beta - Album Three/01 - Track A.flac",
        "Track A",
        "Artist Beta",
        "Album Three",
        1,
        3,
    ),
    TrackSpec(
        "Artist Beta - Album Three/02 - Track B.flac",
        "Track B",
        "Artist Beta",
        "Album Three",
        2,
        3,
    ),
    TrackSpec(
        "Artist Beta - Album Three/03 - Track C.flac",
        "Track C",
        "Artist Beta",
        "Album Three",
        3,
        3,
    ),
]

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def generate(music_dir: Path = MUSIC_DIR, *, overwrite: bool = False) -> None:
    music_dir.mkdir(parents=True, exist_ok=True)

    generated = 0
    skipped = 0

    for spec in LIBRARY:
        dest = music_dir / spec.path
        if dest.exists() and not overwrite:
            print(f"  skip (exists): {spec.path}")
            skipped += 1
            continue

        dest.parent.mkdir(parents=True, exist_ok=True)
        data = make_silent_flac(
            title=spec.title,
            artist=spec.artist,
            album=spec.album,
            track_number=spec.track_number,
            track_total=spec.track_total,
            year=spec.year,
            genre=spec.genre,
        )
        dest.write_bytes(data)
        print(f"  wrote {len(data):>5} bytes: {spec.path}")
        generated += 1

    print(f"\nDone — {generated} generated, {skipped} skipped.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Regenerate files even if they already exist.",
    )
    parser.add_argument(
        "--music-dir",
        type=Path,
        default=MUSIC_DIR,
        help=f"Output directory (default: {MUSIC_DIR})",
    )
    args = parser.parse_args()

    print(f"Generating fixtures in: {args.music_dir}\n")
    generate(args.music_dir, overwrite=args.overwrite)
