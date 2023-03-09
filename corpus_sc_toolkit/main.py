import sys
from pathlib import Path

from corpus_pax import setup_pax
from dotenv import find_dotenv, load_dotenv
from loguru import logger
from pylts import ConfigS3
from sqlpyd import Connection

from .config import ConfigDecisions, ConfigStatutes
from .decisions import decision_storage
from .statutes import statute_storage

load_dotenv(find_dotenv())
logger.configure(
    handlers=[
        {
            "sink": "logs/error.log",
            "format": "{message}",
            "level": "ERROR",
        },
        {
            "sink": "logs/warnings.log",
            "format": "{message}",
            "level": "WARNING",
            "serialize": True,
        },
        {
            "sink": sys.stderr,
            "format": "{message}",
            "level": "DEBUG",
            "serialize": True,
        },
    ]
)
data_folder = Path(__file__).parent.parent / "data"


def get_pdf_db(reset: bool = False, path: Path = data_folder) -> Path:
    src = "s3://corpus-pdf/db"
    logger.info(f"Restore from {src=} to {path=}")
    stream = ConfigS3(s3=src, folder=path)
    if reset:
        stream.delete()
        return stream.restore()
    if not stream.dbpath.exists():
        return stream.restore()
    return stream.dbpath


def config_db(reset: bool = False):
    main_conn: Connection = setup_pax(str(get_pdf_db(reset)))

    config_statutes = ConfigStatutes(conn=main_conn, storage=statute_storage)
    config_statutes.add_rows()

    config_decisions = ConfigDecisions(
        conn=main_conn, storage=decision_storage
    )
    config_decisions.add_rows()
