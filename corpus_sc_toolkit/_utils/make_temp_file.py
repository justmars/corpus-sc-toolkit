from pathlib import Path
from typing import Any

import yaml
from loguru import logger
from start_sdk import CFR2_Bucket

"""Generic temporary file download."""

TEMP_FOLDER = Path(__file__).parent / "tmp"
TEMP_FOLDER.mkdir(exist_ok=True)


def create_temp_yaml(data: dict) -> Path:
    if data.get("id"):
        logger.debug(f"Creating temp file for {data['id']=}")
    temp_path = TEMP_FOLDER / "temp.yaml"
    temp_path.unlink(missing_ok=True)  # delete existing content, if any.
    with open(temp_path, "w+"):
        temp_path.write_text(yaml.safe_dump(data))
    return temp_path


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
