"""NCIT release detection reads a bounded ontology header, not an HTTP date."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests

PLUGIN = Path(__file__).resolve().parents[1] / "plugins" / "ncit"
SPEC = importlib.util.spec_from_file_location("ncit_version", PLUGIN / "version.py")
VERSION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERSION)
SOURCE_URL = "https://example.org/ncit.obo"


@pytest.fixture
def response(monkeypatch):
    response = MagicMock()
    response.__enter__.return_value = response
    response.raw.read.return_value = (
        b"format-version: 1.2\ndata-version: releases/2026-03-19\nontology: ncit\n"
    )
    response.headers = {"Date": "Fri, 20 Nov 2026 07:22:35 GMT"}
    get = MagicMock(return_value=response)
    monkeypatch.setattr(requests, "get", get)
    return response, get


@pytest.mark.parametrize("newline", [b"\n", b"\r\n"])
def test_release_comes_from_bounded_ontology_header(response, newline):
    result, get = response
    result.raw.read.return_value = result.raw.read.return_value.replace(b"\n", newline)

    assert VERSION.get_release(SimpleNamespace(SRC_URLS=[SOURCE_URL])) == "2026-03-19"

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
        b"format-version: 1.2\nontology: ncit\n",
        b"format-version: 1.2\ndata-version: unknown\n",
    ],
)
def test_missing_release_fails_and_closes_response(response, header):
    result, _ = response
    result.raw.read.return_value = header

    with pytest.raises(ValueError, match="missing a release data-version"):
        VERSION.get_release(SimpleNamespace(SRC_URLS=[SOURCE_URL]))

    result.__exit__.assert_called_once()


def test_http_error_fails_before_reading_and_closes_response(response):
    result, _ = response
    result.raise_for_status.side_effect = requests.HTTPError("503 unavailable")

    with pytest.raises(requests.HTTPError, match="503 unavailable"):
        VERSION.get_release(SimpleNamespace(SRC_URLS=[SOURCE_URL]))

    result.raw.read.assert_not_called()
    result.__exit__.assert_called_once()


def test_invalid_release_date_fails_and_closes_response(response):
    result, _ = response
    result.raw.read.return_value = b"data-version: releases/2026-99-99\n"

    with pytest.raises(ValueError):
        VERSION.get_release(SimpleNamespace(SRC_URLS=[SOURCE_URL]))

    result.__exit__.assert_called_once()
