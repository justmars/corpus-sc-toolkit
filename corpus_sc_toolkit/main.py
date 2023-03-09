import sys
from pathlib import Path

from corpus_pax import setup_pax
from dotenv import find_dotenv, load_dotenv
from loguru import logger
from sqlpyd import Connection

from .decisions import ConfigDecisions, decision_storage
from .statutes import ConfigStatutes, statute_storage

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
db_file = data_folder / "lawdata.db"


def config_db(dbpath: str = str(db_file)):
    """Creates/uses database in `dbpath` containing content
    from `corpus_pax` and content from r2 storage buckets."""
    c: Connection = setup_pax(dbpath)
    ConfigStatutes(conn=c, storage=statute_storage).add_rows()
    ConfigDecisions(conn=c, storage=decision_storage).add_rows()
