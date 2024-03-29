"""Test the EPA CEMS data scraping script."""
import os
import random

from pudl_scrapers.bin.epacems import EpaCemsFtpManager, states


class TestEpaCemsFtpManager:
    """Test the EPA CEMS FTP Manager class."""

    def test_initialization(self):
        """Make sure the EpaCemsFtpManager loads."""
        EpaCemsFtpManager(testing=True)

    def test_minimal_collection(self):
        """Ensure EpaCemsFtpManager collects a few files and cleans up after itself."""
        with EpaCemsFtpManager(testing=True) as cftp:
            year = random.randint(1995, 2020)  # nosec: B311
            cftp.collect_year(year)

            output_dir = cftp.output_dir
            assert os.path.exists(output_dir)

        assert not os.path.exists(output_dir)

    def test_parse_name(self):
        """Make sure we can ID the filterable components of a file name."""
        year = random.randint(1995, 2020)  # nosec: B311
        month = random.randint(1, 12)  # nosec: B311
        state = random.choice(states).lower()  # nosec: B311

        filename = f"{year}{state}{month:02}.zip"

        msg = f"wrong result for {filename}"
        epacems = EpaCemsFtpManager()

        assert epacems.file_year(filename) == year, msg
        assert epacems.file_state(filename) == state, msg
