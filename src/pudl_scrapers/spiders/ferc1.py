"""Scrapy spider for downloading FERC Form 1 data."""
from pathlib import Path

import scrapy
from scrapy.http import Request

from pudl_scrapers import items
from pudl_scrapers.helpers import new_output_dir


class Ferc1Spider(scrapy.Spider):
    """Scrapy spider for downloading FERC Form 1 data."""

    name = "ferc1"
    allowed_domains = ["www.ferc.gov"]
    start_urls = [
        "https://www.ferc.gov/general-information-0/electric-industry-forms/form-1-1-f-3-q-electric-historical-vfp-data"
    ]

    def start_requests(self):
        """Start requesting FERC 1 forms.

        Yields:
            List of Requests for FERC 1 forms
        """
        # Spider settings are not available during __init__, so finalizing here
        settings_output_dir = Path(self.settings.get("OUTPUT_DIR"))
        output_root = settings_output_dir / "ferc1"
        self.output_dir = new_output_dir(output_root)

        yield from self.all_form_requests()

    def parse(self, response):
        """Produce the FERC item.

        Args:
            response: scrapy.http.Response containing ferc1 data

        Yields:
            Ferc1 item
        """
        path = self.output_dir / f"ferc1-{response.meta['year']}.zip"

        yield items.Ferc1(
            data=response.body, year=response.meta["year"], save_path=path
        )

    def form_for_year(self, year):
        """Produce a form request for the given year.

        Args:
            year: int

        Returns:
            Request for the Ferc 1 form
        """
        url = f"https://forms.ferc.gov/f1allyears/f1_{year}.zip"
        return Request(url, meta={"year": year}, callback=self.parse)

    def all_form_requests(self):
        """Produces form requests for all supported years.

        Yields:
            Requests for all available Ferc form 1 zip files
        """
        for year in range(1994, 2022):
            yield self.form_for_year(year)
