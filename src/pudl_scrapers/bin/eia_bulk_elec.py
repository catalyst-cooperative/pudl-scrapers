"""A script to download EIA electricity data in bulk.

This script doesn't take any arguments, but does download ~200MB of JSON.

"""
import argparse
import logging
import sys
from pathlib import Path

import requests
from tqdm import tqdm

from pudl_scrapers.helpers import new_output_dir
from pudl_scrapers.settings import OUTPUT_DIR

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DATA_URL = "https://api.eia.gov/bulk/ELEC.zip"


def parse_command_line(argv):
    """Parse the command line args, just to print a help message."""
    parser = argparse.ArgumentParser(description=__doc__)
    return parser.parse_args(argv[1:])


def main() -> int:
    """Download EIA bulk electricity data."""
    _ = parse_command_line(sys.argv)

    output_dir = new_output_dir(Path(OUTPUT_DIR) / "eia_bulk_elec")
    out_path = output_dir / "eia_bulk_elec.zip"
    out_path.parent.mkdir(parents=True, exist_ok=False)

    logger.info("Starting download of EIA bulk electricity data.")

    resp = requests.get(DATA_URL, stream=True)
    resp.raise_for_status()

    total_size_in_bytes = int(resp.headers.get("content-length", 0))
    chunk_size = 2**20  # MB
    progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)
    with open(out_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            progress_bar.update(len(chunk))
            f.write(chunk)
    progress_bar.close()
    logger.info("Completed download of EIA bulk electricity data.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
