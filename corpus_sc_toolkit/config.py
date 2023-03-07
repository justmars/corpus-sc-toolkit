import sys
from pathlib import Path
from sqlite3 import IntegrityError

import yaml
from corpus_pax import Individual, setup_pax
from dotenv import find_dotenv, load_dotenv
from loguru import logger
from pydantic import BaseModel, Field
from pylts import ConfigS3
from sqlite_utils import Database
from sqlpyd import Connection

from .decisions import (
    CitationRow,
    DecisionHTML,
    DecisionRow,
    Justice,
    OpinionRow,
    SegmentRow,
    TitleTagRow,
    VoteLine,
    extract_votelines,
    get_justices_file,
    tags_from_title,
)
from .statutes import Statute

LOCAL_FOLDER = Path().home().joinpath("code/corpus")
STATUTES = LOCAL_FOLDER.glob("statutes/**/details.yaml")
DECISIONS = LOCAL_FOLDER.glob("decisions/**/details.yaml")
DB_FOLDER = Path(__file__).parent.parent / "data"
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


class ConfigStatutes(BaseModel):
    @classmethod
    def extract_local_statutes(cls):
        for detail_path in STATUTES:
            try:
                if obj := Statute.from_page(detail_path):
                    logger.debug(f"Uploading: {obj.id=}")
                    obj.to_storage()
                else:
                    logger.error(f"Error uploading {detail_path=}")
            except Exception as e:
                logger.error(f"Bad {detail_path=}; see {e=}")


class ConfigDecisions(BaseModel):
    """# How to configure

    ```py
        # set up initial PDF tables, takes less than a minute to download
        from corpus_sc_toolkit import ConfigDecisions
        config = ConfigDecisions.setup(reset=True)

        # add to db downloaded: individuals / organizations
        from corpus_pax import setup_pax
        setup_pax(str(config.conn.path_to_db))

        # build decision-focused tables
        config.build_tables()

        # test saving a document from R2
        r2_collection = config.iter_decisions()
        item = next(r2_collection)
        type(item) # DecisionRow
        config.add_decision(item)
    ```
    """

    conn: Connection
    path: Path = Field(default=DB_FOLDER)

    def extract_local_decisions(self):
        for detail_path in DECISIONS:
            try:
                if obj := DecisionHTML.make_from_path(
                    local_path=detail_path, db=self.conn.db
                ):
                    logger.debug(f"Uploading: {obj.id=}")
                    obj.to_storage()
                else:
                    logger.error(f"Error uploading {detail_path=}")
            except Exception as e:
                logger.error(f"Bad {detail_path=}; see {e=}")

    @classmethod
    def get_pdf_db(cls, reset: bool = False) -> Path:
        src = "s3://corpus-pdf/db"
        logger.info(f"Restore from {src=} to {DB_FOLDER=}")
        stream = ConfigS3(s3=src, folder=DB_FOLDER)
        if reset:
            stream.delete()
            return stream.restore()
        if not stream.dbpath.exists():
            return stream.restore()
        return stream.dbpath

    @classmethod
    def setup(cls, reset: bool = False):
        """Get sqlite db containing pdf tables from aws via litestream."""
        dbpath: Path = cls.get_pdf_db(reset=reset)
        conn = Connection(DatabasePath=str(dbpath), WAL=True)
        return cls(conn=conn)

    def build_tables(self) -> Database:
        """Create all relevant tables involving decision object."""
        logger.info("Ensure tables are created.")
        # Populate pax tables so that authors can be associated with decisions
        try:
            justices = yaml.safe_load(get_justices_file().read_bytes())
            self.conn.add_records(Justice, justices)
        except IntegrityError:
            ...  # already existing table because of prior addition
        self.conn.create_table(DecisionRow)
        self.conn.create_table(CitationRow)
        self.conn.create_table(OpinionRow)
        self.conn.create_table(VoteLine)
        self.conn.create_table(TitleTagRow)
        self.conn.create_table(SegmentRow)
        self.conn.db.index_foreign_keys()
        logger.info("Decision-based tables ready.")
        return self.conn.db

    def add_decision(self, row: DecisionRow) -> str | None:
        """This creates a decision row and correlated metadata involving
        the decision, i.e. the citation, voting text, tags from the title, etc.,
        and then add rows for their respective tables.

        Args:
            row (DecisionRow): Uniform fields ready for database insertion

        Returns:
            str | None: The decision id, if the insertion of records is successful.
        """
        table = self.conn.table(DecisionRow)
        try:
            added = table.insert(record=row.dict(), pk="id")  # type: ignore
            logger.debug(f"Added {added.last_pk=}")
        except Exception as e:
            logger.error(f"Skip duplicate: {row.id=}; {e=}")
            return None
        if not added.last_pk:
            logger.error(f"Not made: {row.dict()=}")
            return None

        for email in row.emails:
            table.update(added.last_pk).m2m(
                other_table=self.conn.table(Individual),
                pk="id",
                lookup={"email": email},
                m2m_table="sc_tbl_decisions_pax_tbl_individuals",
            )  # note explicit m2m table name is `sc_`

        if row.citation and row.citation.has_citation:
            self.conn.add_record(kls=CitationRow, item=row.citation_fk)

        if row.voting:
            self.conn.add_records(
                kls=VoteLine,
                items=extract_votelines(
                    decision_pk=added.last_pk, text=row.voting
                ),
            )

        if row.title:
            self.conn.add_records(
                kls=TitleTagRow,
                items=tags_from_title(
                    decision_pk=added.last_pk, text=row.title
                ),
            )

        for op in row.opinions:
            self.conn.add_record(kls=OpinionRow, item=op.dict())
            self.conn.add_records(
                kls=SegmentRow, items=list(op.dict() for op in op.segments)
            )

        return row.id

    @classmethod
    def restart(cls):
        config = cls.setup(reset=True)
        setup_pax(str(config.conn.path_to_db))
        config.build_tables()
        objs = DecisionRow.from_cloud_storage(db=config.conn.db)
        for index, item in enumerate(objs):
            logger.info(f"{item.id=}; {index=}")
            if decision_added := config.add_decision(item):
                logger.success(f"{decision_added=}")
            else:
                logger.error(f"{item.id=}")
