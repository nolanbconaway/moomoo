from pathlib import Path

VERSION = (Path(__file__).resolve().parent / "version").read_text().strip()
