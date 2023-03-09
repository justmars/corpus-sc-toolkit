import abc
from collections.abc import Iterator
from pathlib import Path

from loguru import logger
from pydantic import BaseModel
from pylts import ConfigS3
from sqlite_utils import Database
from sqlpyd import Connection
from start_sdk import StorageUtils


class StorageToDatabaseConfiguration(BaseModel, abc.ABC):
    """Each flow must implement 4 functions:

    1. `set_tables()` using the `sqlpyd.Connection` convention
    2. `add_row()` expects a Pydantic model instance already converted
        with fields that can be used in the tables created / assigned in
        `set_tables()`
    3. `add_rows()` retrieves bucket prefixes `start_sdk.StorageUtils` and
        converts these prefixes to Pydantic model instances so that
        `add_row()` can be used
    4. `get_rows()` lists ids of the main model
    """

    conn: Connection
    storage: StorageUtils

    @abc.abstractmethod
    def set_tables(self) -> None:
        """Prep tables for data entry. The tables created here will be utilized in
        `add_row()`"""
        raise NotImplementedError

    @abc.abstractmethod
    def add_row(self) -> None:
        """Implies prior creation of tables under`set_tables()`, will accept an instance
        retrieved from storage to add the same to the database."""
        raise NotImplementedError

    @abc.abstractmethod
    def add_rows(self) -> None:
        """Using storage items converted into rows add each to database."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_rows(self) -> None:
        """Get existing ids of main table associated with the storage bucket, e.g.

        Main Table | Storage Bucket
        --:|:--
        DecisionRow | `sc-decisions`
        Statute | `ph-statutes`
        """
        raise NotImplementedError


LOCAL_FOLDER = Path().home().joinpath("code/corpus")


def store_local_statutes_in_r2(
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


def store_local_decisions_in_r2(
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
                if DecisionHTML.get_key(obj.prefix):
                    logger.debug(f"Skipping: {obj.prefix=}")
                    continue

                logger.debug(f"Uploading: {obj.id=}")
                obj.to_storage()
            else:
                logger.error(f"Error uploading {detail_path=}")
        except Exception as e:
            logger.error(f"Bad {detail_path=}; see {e=}")


def get_pdf_db(path: Path, reset: bool = False) -> Path:
    """Download pre-existing database containing pdf tables. This is
    needed to transfer pdf-based rows to r2 and to another database."""
    src = "s3://corpus-pdf/db"
    logger.info(f"Restore from {src=} to {path=}")
    stream = ConfigS3(s3=src, folder=path)
    if reset:
        stream.delete()
        return stream.restore()
    if not stream.dbpath.exists():
        return stream.restore()
    return stream.dbpath


def store_pdf_decisions_in_r2(pdf_db: Database):
    """Used in tandem with `get_pdf_db()`; retrieves the records
    so that these can be uploaded. Note that this will overwrite
    present fields."""
    from .decisions import DecisionPDF

    for row in DecisionPDF.originate(db=pdf_db):
        try:
            row.to_storage()
        except Exception as e:
            logger.error(f"Bad {row.id=}; see {e=}")
