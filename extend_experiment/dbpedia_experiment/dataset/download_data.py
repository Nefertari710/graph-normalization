#!/usr/bin/env python3
"""Download the four English DBpedia 2022.12.01 files from official hosting."""

import argparse
import bz2
from http.client import HTTPException
from pathlib import Path
import sys
from urllib.request import urlopen


RELEASE = "2022.12.01"
BASE_URL = "https://downloads.dbpedia.org/repo/dbpedia"
SOURCES = {
    "instance-types_lang=en_specific.ttl.bz2": "mappings/instance-types",
    "labels_lang=en.ttl.bz2": "generic/labels",
    "mappingbased-literals_lang=en.ttl.bz2": "mappings/mappingbased-literals",
    "mappingbased-objects_lang=en.ttl.bz2": "mappings/mappingbased-objects",
}
CHUNK_SIZE = 1024 * 1024


def check_archive(path: Path) -> None:
    """Check the full bzip2 stream without extracting a file onto disk."""
    with bz2.open(path, "rb") as stream:
        if not stream.read(CHUNK_SIZE):
            raise ValueError(f"Empty archive: {path}")
        while stream.read(CHUNK_SIZE):
            pass


def download_file(url: str, destination: Path) -> None:
    if destination.exists():
        print(f"Checking existing file: {destination.name}", flush=True)
        try:
            check_archive(destination)
        except (OSError, EOFError, ValueError) as exc:
            raise ValueError(
                f"Invalid existing archive: {destination}. Move or remove it and retry."
            ) from exc
        print("Kept existing file (release identity is not checked).", flush=True)
        return

    temporary = destination.with_name(destination.name + ".tmp")
    print(f"Downloading: {url}", flush=True)
    try:
        with urlopen(url, timeout=60) as response, temporary.open("wb") as output:
            expected_size = response.headers.get("Content-Length")
            received = 0
            while chunk := response.read(CHUNK_SIZE):
                output.write(chunk)
                received += len(chunk)
        if expected_size is not None and received != int(expected_size):
            raise ValueError(f"Incomplete download: {destination.name}")
        check_archive(temporary)
        temporary.replace(destination)
        print(f"Saved: {destination.name} ({received:,} bytes)", flush=True)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir", type=Path, default=Path(__file__).resolve().parent / "data",
        help="Output directory (default: dataset/data/ beside this script).",
    )
    args = parser.parse_args()
    try:
        args.data_dir.mkdir(parents=True, exist_ok=True)
        for filename, source in SOURCES.items():
            download_file(f"{BASE_URL}/{source}/{RELEASE}/{filename}", args.data_dir / filename)
    except (OSError, EOFError, ValueError, HTTPException) as exc:
        print(f"Download failed: {exc}\nFix the problem and rerun the same command.", file=sys.stderr)
        return 1
    print("All four archives are ready. Run import_data.py next.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
