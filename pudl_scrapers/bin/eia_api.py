"""Script to download data from the EIA API.

The EIA provides a REST API to access much of their data. Queries can by passed by URL routes,
but because they can get very long, it is more readable to use the other option of appending
JSON to the request header.
"""
import gzip
import json
import logging
import os
from shutil import copyfileobj
import sys
from copy import deepcopy
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence, Union
from warnings import warn

import requests
from dotenv import load_dotenv
from tqdm import tqdm

from pudl_scrapers.helpers import new_output_dir
from pudl_scrapers.settings import OUTPUT_DIR

load_dotenv(Path(__file__).resolve().parents[2] / "eia_api.env")

logger = logging.Logger(__name__, level="INFO")


class EIAAPI(object):
    API_ROOT_URL = "https://api.eia.gov/v2/"
    ELECTRIC_POWER_ROUTE = "electricity/electric-power-operational-data/data/"
    API_VERSION = "2.0.2"

    def __init__(self) -> None:
        settings_output_dir = Path(OUTPUT_DIR)
        output_root = settings_output_dir / "eia_api"
        self.output_dir = new_output_dir(output_root)

        self.session = requests.Session()
        # enable json X-Params
        self.session.headers["Content-Type"] = "application/json"
        self.electric_power_url = EIAAPI.API_ROOT_URL + EIAAPI.ELECTRIC_POWER_ROUTE + self._generate_api_key_query()
        return

    def _generate_api_key_query(self) -> str:
        try:
            api_key_eia = os.environ["API_KEY_EIA"]
        except KeyError:
            raise Exception("API_KEY_EIA not found. It must be specified in a repo level file called eia_api.env")
        return f"?api_key={api_key_eia}"

    def get(self, url, param_dict: Dict[str, Any], **kwargs) -> requests.Response:
        """Thin wrapper on requests.Session.get() that encodes param_dict as JSON in the header."""
        response = self.session.get(url, headers={"X-Params": json.dumps(param_dict)}, **kwargs)
        return response

    def get_fuel_receipts_costs_aggregates(
        self,
        *,
        frequency: Literal["annual", "monthly"] = "annual",
        data: Optional[Sequence[str]] = ("cost-per-btu", "receipts-btu"),
        start: Optional[str] = "2001",
        end: Optional[str] = None,
        offset: int = 0,
        length: int = 5000,  # Max 5000
        facets: Optional[Dict[str, List[Union[str, int]]]] = None,
        sort: Optional[List[Dict[Literal["column", "direction"], str]]] = None,
        out_path: Optional[Path] = None,
    ) -> None:
        """Python implementation of EIA API parameters.

        See their query browser for details:
        https://www.eia.gov/opendata/browser/electricity/electric-power-operational-data
        """
        data = list(data) if data else []
        exclude_locals = {"self", "out_path", "exclude_locals"}
        # default behavior is no filtering, so drop None and empty list
        param_dict = {k: v for k, v in locals().items() if ((bool(v) or v == 0) and k not in exclude_locals)}
        if out_path is None:
            out_path = self.output_dir / ("_".join([frequency, *data, str(start)]) + ".json")
            out_path.parent.mkdir(parents=True, exist_ok=True)

        # Write each response as a new item in a JSON array.
        # This includes not just data but also request and data metadata.
        with out_path.open("w") as file:
            # initialize file as JSON array
            file.write("[")
            self._get_all_values(url=self.electric_power_url, param_dict=param_dict, file_obj=file)
            file.write("]")  # close JSON array
        gzip_file(out_path, delete_uncompressed=True)
        return

    def _get_all_values(self, *, url, param_dict: Dict[str, Any], file_obj: TextIOWrapper) -> None:
        """Iterate through a big result set at the rate set by param_dict['length']."""
        first_response = self.get(url, param_dict)
        first_response.raise_for_status()
        resp_json = first_response.json()
        self._check_api_version(resp_json)

        logger.debug(f"Params passed by user: {param_dict}")
        logger.debug(f"Params request: {first_response.request.headers['X-Params']}")
        logger.debug(f"Params interpreted by API: {resp_json['request']['params']}")

        n_total_results = resp_json["response"]["total"]
        n_requests = -(-n_total_results // param_dict["length"])  # round up
        logger.info(f"Downloading {n_total_results} points from EIA API via {n_requests} requests.")

        file_obj.write(first_response.text)

        # responses 2 through N
        new_params = deepcopy(param_dict)
        for i in tqdm(range(1, n_requests), unit_scale=param_dict["length"], unit=" points"):
            offset = param_dict["length"] * i
            new_params["offset"] = offset
            response = self.get(url, param_dict=new_params)
            file_obj.write(f",\n{response.text}")

        return

    def _check_api_version(self, response_json: Dict[str, Any]):
        try:
            observed_api_version = response_json["apiVersion"]
        except KeyError:
            warn("API has changed: version number no longer accessible.")
        expected_parts = EIAAPI.API_VERSION.split(".")
        observed_parts = observed_api_version.split(".")
        if expected_parts[0] != observed_parts[0]:
            warn(
                f"Major API version change - check that this data is still what we want! Expected: {EIAAPI.API_VERSION}, Observed: {observed_api_version}"
            )
        elif expected_parts[1] != observed_parts[1]:
            warn(
                f"Minor API version change - check that this data is still what we want! Expected: {EIAAPI.API_VERSION}, Observed: {observed_api_version}"
            )
        elif expected_parts[2] != observed_parts[2]:
            logger.info(
                f"Patch level version change in EIA API. Please update expected version. Expected: {EIAAPI.API_VERSION}, Observed: {observed_api_version}"
            )
        else:
            return
        return


def gzip_file(filepath: Path, delete_uncompressed=False) -> None:
    """Compress a file and optionally delete the uncompressed version."""
    with open(filepath, "rb") as f_in:
        with gzip.open(filepath.with_suffix(filepath.suffix + ".gz"), "wb") as f_out:
            copyfileobj(f_in, f_out)
    if delete_uncompressed:
        filepath.unlink()
    return


def main():
    """Download monthly and annual fuel receipts and cost aggregates."""
    annual_params = {
        "frequency": "annual",
        "data": ["cost-per-btu", "receipts-btu"],
        # tiny subset for testing
        # "facets": {"fueltypeid": ["NG"], "location": ["US"], "sectorid": [98]},
        "start": "2001",
        "end": None,
        # "sort": [],
        "offset": 0,
        "length": 5000,  # max 5000
    }
    monthly_params = deepcopy(annual_params)
    monthly_params["frequency"] = "monthly"

    api = EIAAPI()
    api.get_fuel_receipts_costs_aggregates(**annual_params)
    api.get_fuel_receipts_costs_aggregates(**monthly_params)
    return


if __name__ == "__main__":
    sys.exit(main())
