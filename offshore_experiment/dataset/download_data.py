#!/usr/bin/env python3
"""Download and extract the six Offshore Leaks CSVs from ICIJ's official ZIP."""

import argparse
import csv
from http.client import HTTPException
from pathlib import Path, PurePosixPath
import shutil
import sys
from tempfile import TemporaryDirectory
from urllib.request import urlopen
from zipfile import BadZipFile, ZipFile


DOWNLOAD_URL = "https://offshoreleaks-data.icij.org/offshoreleaks/csv/full-oldb.LATEST.zip"
FILES = (
    "nodes-addresses.csv",
    "nodes-entities.csv",
    "nodes-intermediaries.csv",
    "nodes-officers.csv",
    "nodes-others.csv",
    "relationships.csv",
)
CHUNK_SIZE = 1024 * 1024


def check_csv(path: Path) -> None:
    expected = (
        ["node_id_start", "node_id_end", "rel_type"]
        if path.name == "relationships.csv" else ["node_id"]
    )
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream)
        header = next(reader, [])
        if header[:len(expected)] != expected or not next(reader, []):
            raise ValueError(f"Missing data or unexpected CSV header: {path}")


def download_archive(destination: Path) -> None:
    print(f"Downloading: {DOWNLOAD_URL}", flush=True)
    with urlopen(DOWNLOAD_URL, timeout=60) as response, destination.open("wb") as output:
        expected_size = response.headers.get("Content-Length")
        received = 0
        while chunk := response.read(CHUNK_SIZE):
            output.write(chunk)
            previous = received
            received += len(chunk)
            if received // (16 * CHUNK_SIZE) != previous // (16 * CHUNK_SIZE):
                print(f"  {received / CHUNK_SIZE:.0f} MiB downloaded", flush=True)
    if expected_size is not None and received != int(expected_size):
        raise ValueError("Incomplete ZIP download; rerun the command to retry.")


def extract_csvs(archive_path: Path, staging: Path) -> None:
    with ZipFile(archive_path) as archive:
        members = {}
        for member in archive.infolist():
            filename = PurePosixPath(member.filename).name
            if not member.is_dir() and filename in FILES:
                if filename in members:
                    raise ValueError(f"Duplicate CSV filename in ZIP: {filename}")
                members[filename] = member
        missing = set(FILES) - members.keys()
        if missing:
            raise ValueError("CSV files missing from ZIP: " + ", ".join(sorted(missing)))
        for filename in FILES:
            print(f"Extracting and checking: {filename}", flush=True)
            # Select by basename and write to our own paths; do not extractall().
            # Reading each complete member also checks its ZIP CRC.
            target = staging / filename
            with archive.open(members[filename]) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output, CHUNK_SIZE)
            check_csv(target)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir", type=Path, default=Path(__file__).resolve().parent / "data",
        help="Output directory (default: offshore_experiment/dataset/data/).",
    )
    args = parser.parse_args(argv)
    data_dir = args.data_dir.expanduser().resolve()
    try:
        existing = [filename for filename in FILES if (data_dir / filename).exists()]
        if existing:
            if len(existing) != len(FILES):
                raise ValueError(
                    "Only some expected CSV files exist. Move them aside or choose "
                    "an empty --data-dir before downloading a complete snapshot."
                )
            for filename in FILES:
                check_csv(data_dir / filename)
            print("All six CSVs already exist; kept existing data without downloading.")
            print("Only CSV headers and the first data rows were checked, not snapshot identity.")
            return 0
        if data_dir.exists() and any(data_dir.iterdir()):
            raise ValueError("The output directory must be empty before downloading a new dataset.")
        data_dir.parent.mkdir(parents=True, exist_ok=True)
        # Keep download/extraction temporary; publish the validated set atomically.
        # The .tmp directory is ignored by Git and removed after failures too.
        with TemporaryDirectory(prefix="offshore-download-", suffix=".tmp", dir=data_dir.parent) as scratch:
            staging = Path(scratch)
            archive_path = staging / "offshore.zip"
            csv_staging = staging / "data"
            csv_staging.mkdir()
            download_archive(archive_path)
            extract_csvs(archive_path, csv_staging)
            if any((data_dir / filename).exists() for filename in FILES):
                raise ValueError("CSV files appeared during download; existing data were left unchanged.")
            csv_staging.rename(data_dir)
        print(f"All six CSVs are ready in {data_dir}. Run the import script next.")
        return 0
    except KeyboardInterrupt:
        print("\nDownload cancelled. Temporary files were removed; rerun to retry.", file=sys.stderr)
        return 130
    except (OSError, ValueError, EOFError, HTTPException, BadZipFile, csv.Error, RuntimeError) as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
