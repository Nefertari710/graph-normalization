#!/usr/bin/env python3
"""Download the pinned CMS Provider and Service CSV for calendar year 2024."""

import argparse
import csv
from http.client import HTTPException
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from urllib.request import urlopen


FILENAME = "PHY_R26_P05_V10_D24_Prov_Svc.csv"
DOWNLOAD_URL = (
    "https://data.cms.gov/sites/default/files/2026-05/"
    "b5ebab5a-f490-418a-9bce-4b9f31419356/" + FILENAME
)
# Size published by CMS for this specific file (not a moving latest release):
# https://data.cms.gov/data-api/v1/dataset-resources/335e5f35-eca6-482d-87b3-f99883e213e3
EXPECTED_SIZE = 3_250_282_192
REQUIRED_COLUMNS = {
    "rndrng_npi", "hcpcs_cd", "place_of_srvc",
    "rndrng_prvdr_last_org_name", "rndrng_prvdr_first_name", "rndrng_prvdr_mi",
    "rndrng_prvdr_ent_cd", "rndrng_prvdr_st1", "rndrng_prvdr_st2",
    "rndrng_prvdr_city", "rndrng_prvdr_state_abrvtn", "rndrng_prvdr_zip5",
    "rndrng_prvdr_cntry", "hcpcs_desc", "hcpcs_drug_ind",
}
CHUNK_SIZE = 1024 * 1024


def check_csv(path: Path) -> None:
    size = path.stat().st_size
    if size != EXPECTED_SIZE:
        raise ValueError(f"Unexpected CSV size: {size:,} bytes; expected {EXPECTED_SIZE:,}.")
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        headers = [value.lower().replace(" ", "_").replace("/", "_") for value in next(reader, [])]
        missing = REQUIRED_COLUMNS - set(headers)
        if missing:
            raise ValueError("CSV is missing required columns: " + ", ".join(sorted(missing)))
        row = next(reader, [])
        if len(row) != len(headers):
            raise ValueError("CSV has no complete first data row.")


def download_csv(destination: Path) -> None:
    print(f"Downloading: {DOWNLOAD_URL}", flush=True)
    with urlopen(DOWNLOAD_URL, timeout=60) as response, destination.open("wb") as output:
        content_length = response.headers.get("Content-Length")
        if content_length is not None and int(content_length) != EXPECTED_SIZE:
            raise ValueError("CMS returned an unexpected file size for the pinned download.")
        received = 0
        while chunk := response.read(CHUNK_SIZE):
            output.write(chunk)
            previous = received
            received += len(chunk)
            if received > EXPECTED_SIZE:
                raise ValueError("Download is larger than the pinned CMS file.")
            if received // (64 * CHUNK_SIZE) != previous // (64 * CHUNK_SIZE):
                print(f"  {received / CHUNK_SIZE:.0f} MiB downloaded", flush=True)
    check_csv(destination)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir", type=Path, default=Path(__file__).resolve().parent / "data",
        help="Output directory (default: dataset/data/ beside this script).",
    )
    args = parser.parse_args(argv)
    data_dir = args.data_dir.expanduser().resolve()
    destination = data_dir / FILENAME
    try:
        if destination.exists():
            check_csv(destination)
            print(f"Kept existing CSV: {destination}")
            print("Size, required columns and the first row passed; source hashes were not compared.")
            return 0
        data_dir.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix="provider-download-", suffix=".tmp", dir=data_dir) as scratch:
            temporary = Path(scratch) / FILENAME
            download_csv(temporary)
            if destination.exists():
                raise ValueError("A CSV appeared during download; existing data were left unchanged.")
            # A hard link publishes the complete file without replacing an
            # existing destination, even if another download finishes first.
            destination.hardlink_to(temporary)
        print(f"CSV is ready: {destination}. Run import_database.py next.")
        return 0
    except KeyboardInterrupt:
        print("\nDownload cancelled. Temporary files were removed; rerun to retry.", file=sys.stderr)
        return 130
    except (OSError, ValueError, EOFError, HTTPException, csv.Error) as exc:
        print(f"Download failed: {exc}\nExisting files were not overwritten.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
