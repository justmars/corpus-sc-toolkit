from collections.abc import Iterator
from pathlib import Path

from loguru import logger
from sqlite_utils import Database

LOCAL_FOLDER = Path().home().joinpath("code/corpus")


def extract_local_statutes(
    path_to_statutes: Iterator[Path] = LOCAL_FOLDER.glob(
        "statutes/**/details.yaml"
    ),
):
    from .statutes import Statute

    for detail_path in path_to_statutes:
        try:
            if obj := Statute.from_page(detail_path):
                logger.debug(f"Uploading: {obj.id=}")
                obj.to_storage()
            else:
                logger.error(f"Error uploading {detail_path=}")
        except Exception as e:
            logger.error(f"Bad {detail_path=}; see {e=}")


def extract_local_decisions(
    db: Database,
    path_to_decisions: Iterator[Path] = LOCAL_FOLDER.glob(
        "decisions/**/details.yaml"
    ),
):
    from .decisions import DecisionHTML

    for detail_path in path_to_decisions:
        try:
            if obj := DecisionHTML.make_from_path(
                local_path=detail_path, db=db
            ):
                if DecisionHTML.key_raw(obj.prefix):
                    logger.debug(f"Skipping: {obj.prefix=}")
                    continue

                logger.debug(f"Uploading: {obj.id=}")
                obj.to_storage()
            else:
                logger.error(f"Error uploading {detail_path=}")
        except Exception as e:
            logger.error(f"Bad {detail_path=}; see {e=}")
