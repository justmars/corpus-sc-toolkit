import os
from collections.abc import Iterator
from http import HTTPStatus
from pathlib import Path

import httpx
import yaml
from dotenv import find_dotenv, load_dotenv
from loguru import logger

from .justice_model import Justice

load_dotenv(find_dotenv())


JUSTICE_LOCAL = Path(__file__).parent / "sc.yaml"
GITHUB_JUSTICES_URL = "https://api.github.com/repos/justmars/corpus/contents/justices/sc.yaml"  # noqa: E501
GITHUB_HEADERS = headers = {
    "Accept": "application/vnd.github.raw",
    "Authorization": f"token {os.getenv('GH_TOKEN')}",
    "X-GitHub-Api-Version": "2022-11-28",
}


def get_justices_from_api() -> Iterator[dict]:
    """A master list of [Justices][justice] is found in the github `/corpus` repository.
    Provided a `GH_TOKEN` is declared as an environment variable,
    this will pull that list into an iterator of dicts.

    Yields:
        Iterator[dict]: Justices from the API
    """
    logger.debug("Extracting justice list from API.")
    with httpx.Client() as client:
        res = client.get(
            url=GITHUB_JUSTICES_URL,
            headers=GITHUB_HEADERS,
            timeout=120,
        )
        if res.status_code == HTTPStatus.OK:
            yield from yaml.safe_load(res.content)
            return
        raise Exception(f"Could not get justice list, see {res=}")


def get_justices_file(local_file: Path = JUSTICE_LOCAL) -> Path:
    """Return, if existing, the path to the `local_file` (*.yaml) containing
    a list of validated [Justices][justice]; if it doesn't exist yet, create it by
    calling [get_justices_from_api()][source-list-from-api].

    Args:
        local_file (Path, optional): _description_. Defaults to JUSTICE_LOCAL.

    Examples:
        >>> from pathlib import Path
        >>> p = Path().cwd() / "tests" / "sc.yaml" # the test file
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
