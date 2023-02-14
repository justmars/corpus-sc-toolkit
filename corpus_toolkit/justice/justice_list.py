import os
from http import HTTPStatus

import httpx
import yaml
from dotenv import find_dotenv, load_dotenv
from loguru import logger

load_dotenv(find_dotenv())


def get_justices_from_api():
    logger.debug("Extracting justice list from API.")
    headers = {
        "Accept": "application/vnd.github.raw",
        "Authorization": f"token {os.getenv('GH_TOKEN')}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    with httpx.Client() as client:
        url = "https://api.github.com/repos/justmars/corpus/contents/justices/sc.yaml"
        res = client.get(url=url, headers=headers, timeout=120)
        if res.status_code == HTTPStatus.OK:
            yield from yaml.safe_load(res.content)
            return
        raise Exception(f"Could not get justice list, see {res=}")


LIST_SC_JUSTICES = list(get_justices_from_api())
