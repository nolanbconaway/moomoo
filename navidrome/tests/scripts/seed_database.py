import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from navidrome_container import SEED_DB, NavidromeContainer


def main():
    with (
        tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as data_dir,
        NavidromeContainer(Path(data_dir)) as container,
    ):
        container.wait_for_http()
        container.wait_for_scan()

        _, output = container.get_wrapped_container().exec_run(
            "sqlite3 /data/navidrome.db 'PRAGMA wal_checkpoint(TRUNCATE)'"
        )
        print("checkpoint:", output.decode())

        print("Extracting DB...")
        SEED_DB.write_bytes(container.extract_db())

    print(f"Done. Commit {SEED_DB} to version control.")


if __name__ == "__main__":
    try:
        main()
    except PermissionError as exc:
        print(f"PermissionError: {exc}. Probably ok to ignore.")
