from pathlib import Path
from typing import Any

import yaml
from start_sdk import CFR2_Bucket

"""Generic temporary file download."""

TEMP_FOLDER = Path(__file__).parent.parent / "tmp"
TEMP_FOLDER.mkdir(exist_ok=True)


def create_temp_yaml(data: dict) -> Path:
    f = TEMP_FOLDER / "temp.yaml"
    f.unlink(missing_ok=True)
    with open(f, "w+"):
        f.write_text(yaml.safe_dump(data))
    return f


def download_to_temp(
    bucket: CFR2_Bucket, src: str, ext: str = "yaml"
) -> str | dict[str, Any] | None:
    """Based on the `src` prefix, download the same into a temp file
    and return its contents based on the extension.

    A `yaml` extension should result in contents in `dict` format;

    An `md` or `html` extension results in `str`.

    The temp file is deleted after every successful extraction of
    the `src` as content."""

    path = TEMP_FOLDER / f"temp.{ext}"
    bucket.download(src, str(path))
    content = None
    if ext == "yaml":
        content = yaml.safe_load(path.read_bytes())
    elif ext in ["md", "html"]:
        content = path.read_text()
    path.unlink(missing_ok=True)
    return content
