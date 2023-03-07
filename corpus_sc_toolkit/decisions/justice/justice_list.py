from collections.abc import Iterator
from http import HTTPStatus
from pathlib import Path

import yaml
from corpus_pax.github import gh
from loguru import logger

from .justice_model import Justice


def get_justices_from_api() -> Iterator[dict]:
    """Need `GH_TOKEN` to copy from /corpus/justices/sc.yaml to iterator of dicts.

    Yields:
        Iterator[dict]: Justices from API
    """
    logger.debug("Extracting justice list from API.")
    res = gh.get(
        "https://api.github.com/repos/justmars/corpus/contents/justices/sc.yaml"
    )
    if res.status_code == HTTPStatus.OK:
        yield from yaml.safe_load(res.content)
    raise Exception(f"No justice list, see {res=}")


def get_justices_file(
    local_file: Path = Path(__file__).parent / "sc.yaml",
) -> Path:
    """Return, if existing, the path to the `local_file` (*.yaml) containing
    a list of validated [Justices][justice]; if it doesn't exist yet, create it by
    calling [get_justices_from_api()][source-list-from-api].

    Args:
        local_file (Path, optional): Path to justice list. See default folder /"sc.yaml"

    Examples:
        >>> from pathlib import Path
        >>> p = Path().cwd() / "justice" / "sc.yaml" # the test file
        >>> f = get_justices_file(p)
        >>> f.exists()
        True

    Returns:
        Path: Yaml file containing list of justices
    """
    if local_file.exists():
        logger.debug("Local justice list file used.")
        return local_file

    with open(local_file, "w+") as writefile:
        yaml.safe_dump(
            data=[
                Justice.from_data(justice_data).dict(exclude_none=True)
                for justice_data in get_justices_from_api()
            ],
            stream=writefile,
            sort_keys=False,
            default_flow_style=False,
        )
        return local_file
