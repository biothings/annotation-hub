"""HPO release detection uses the configured ontology's release header."""

import importlib.util
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests

PLUGIN = Path(__file__).resolve().parents[1] / "plugins" / "hpo"
SPEC = importlib.util.spec_from_file_location("hpo_version", PLUGIN / "version.py")
VERSION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERSION)
SOURCE_URL = "https://example.org/hp.obo"


@pytest.fixture
def response(monkeypatch):
    response = MagicMock()
    response.__enter__.return_value = response
    response.raw.read.return_value = (
        b"format-version: 1.2\ndata-version: hp/releases/2026-09-01\nontology: hp.obo\n"
    )
    response.headers = {"Date": "Fri, 20 Nov 2026 08:09:27 GMT"}
    get = MagicMock(return_value=response)
    monkeypatch.setattr(requests, "get", get)
    return response, get


@pytest.mark.parametrize("newline", [b"\n", b"\r\n"])
@pytest.mark.parametrize("prefix", [b"hp/releases/", b"releases/"])
def test_release_uses_ontology_version_instead_of_http_date(response, newline, prefix):
    result, get = response
    result.raw.read.return_value = result.raw.read.return_value.replace(
        b"hp/releases/", prefix
    ).replace(b"\n", newline)

    assert VERSION.get_release(SimpleNamespace(SRC_URLS=[SOURCE_URL])) == "2026-09-01"

    get.assert_called_once_with(
        SOURCE_URL,
        headers={"Range": "bytes=0-8191", "Accept-Encoding": "identity"},
        stream=True,
        timeout=30,
    )
    result.raise_for_status.assert_called_once_with()
    result.raw.read.assert_called_once_with(8192, decode_content=True)
    result.__exit__.assert_called_once()


@pytest.mark.parametrize(
    "header",
    [
        b"format-version: 1.2\nontology: hp.obo\n",
        b"data-version: unknown\n",
        b"data-version: hp/releases/2026-99-99\n",
    ],
)
def test_invalid_release_fails_without_using_http_date(response, header):
    result, _ = response
    result.raw.read.return_value = header

    with pytest.raises(ValueError):
        VERSION.get_release(SimpleNamespace(SRC_URLS=[SOURCE_URL]))

    result.__exit__.assert_called_once()


def test_http_error_fails_before_reading_and_closes_response(response):
    result, _ = response
    result.raise_for_status.side_effect = requests.HTTPError("503 unavailable")

    with pytest.raises(requests.HTTPError, match="503 unavailable"):
        VERSION.get_release(SimpleNamespace(SRC_URLS=[SOURCE_URL]))

    result.raw.read.assert_not_called()
    result.__exit__.assert_called_once()


def test_release_function_can_be_copied_by_manifest_loader(response):
    namespace = {}
    exec(inspect.getsource(VERSION.get_release), namespace)

    assert namespace["get_release"](SimpleNamespace(SRC_URLS=[SOURCE_URL])) == "2026-09-01"
