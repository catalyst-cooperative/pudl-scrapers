# -*- coding: utf-8 -*-
from pathlib import Path

import factory
from scrapy.http import Request, TextResponse

BASE_PATH = Path(__file__).parent


class RequestFactory(factory.Factory):
    class Meta:
        model = Request

    url = "http://example.com"

    @factory.post_generation
    def meta(obj, create, extracted, **kwargs):
        if extracted is not None:
            for k, v in extracted.items():
                obj.meta[k] = v


def test_path(filename):
    return BASE_PATH / "data" / filename


class FakeResponse(TextResponse):
    """Fake a response for spider testing"""

    def __init__(self, url, file_path, *args, **kwargs):

        with open(file_path, "rb") as f:
            contents = f.read()

        super().__init__(url, *args, body=contents, **kwargs)


class TestResponseFactory(factory.Factory):
    class Meta:
        model = FakeResponse
        inline_args = ("url", "file_path")

    class Params:
        eia860 = factory.Trait(
            url="https://www.eia.gov/electricity/data/eia860/",
            file_path=test_path("eia860.html"))

        eia860m = factory.Trait(
            url="https://www.eia.gov/electricity/data/eia860m/",
            file_path=test_path("eia860m.html"))

        eia861 = factory.Trait(
            url="https://www.eia.gov/electricity/data/eia861/",
            file_path=test_path("eia861.html"))

        eia923 = factory.Trait(
            url="https://www.eia.gov/electricity/data/eia923/",
            file_path=test_path("eia923.html"))

        ferc1 = factory.Trait(
            url="https://www.ferc.gov/docs-filing/forms/form-1/data.asp",
            file_path=test_path("ferc1.html"))

        epaipm = factory.Trait(
            url="https://www.epa.gov/airmarkets/"
                "national-electric-energy-data-system-needs-v6",
            file_path=test_path("epaipm.html"))

    encoding = "utf-8"
    request = factory.SubFactory(
        RequestFactory, url=factory.SelfAttribute("..url"))
