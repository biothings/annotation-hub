# Modified for Annotation Hub: identify the ontology release from its header.
def get_release(self):
    """Read the actual OBO release without downloading the full ontology."""

    import datetime
    import re

    import requests

    with requests.get(
        self.SRC_URLS[0],
        headers={"Range": "bytes=0-8191", "Accept-Encoding": "identity"},
        stream=True,
        timeout=30,
    ) as response:
        response.raise_for_status()
        header = response.raw.read(8192, decode_content=True).decode(
            "utf-8", errors="replace"
        )

    match = re.search(
        r"^data-version:[ \t]*releases/(\d{4}-\d{2}-\d{2})[ \t]*\r?$",
        header,
        flags=re.MULTILINE,
    )
    if match is None:
        raise ValueError("NCIT ontology header is missing a release data-version")
    return datetime.date.fromisoformat(match.group(1)).isoformat()
