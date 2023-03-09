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
from .statutes import (
    Statute,
    StatuteFoundInUnit,
    StatuteMaterialPath,
    StatuteRow,
    StatuteTitleRow,
    StatuteUnitSearch,
)

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


DB_FOLDER = Path(__file__).parent.parent / "data"
load_dotenv(find_dotenv())


class ConfigStatutes(BaseModel):
    conn: Connection
    path: Path = Field(default=DB_FOLDER)

    def build_tables(self):
        """Create all relevant tables involving statute object."""
        self.conn.create_table(StatuteRow)
        self.conn.create_table(StatuteTitleRow)
        self.conn.create_table(StatuteUnitSearch)
        self.conn.create_table(StatuteMaterialPath)
        self.conn.create_table(StatuteFoundInUnit)
        self.conn.db.index_foreign_keys()
        logger.info("Statute-based tables ready.")
        return self.conn.db

    def add(self, statute: Statute):
        # id should be modified prior to adding to db
        record = statute.meta.dict(exclude={"emails"})
        record["id"] = statute.id  # see TODO in Statute
        self.conn.add_record(StatuteRow, record)

        for email in statute.emails:
            self.conn.table(StatuteRow).update(statute.id).m2m(
                other_table=self.conn.table(Individual),
                lookup={"email": email},
                pk="id",
            )

        self.conn.add_cleaned_records(
            kls=StatuteMaterialPath,
            items=statute.material_paths,
        )

        self.conn.add_cleaned_records(
            kls=StatuteUnitSearch,
            items=statute.unit_fts,
        )

        self.conn.add_cleaned_records(
            kls=StatuteTitleRow,
            items=statute.titles,
        )

        self.conn.add_cleaned_records(
            kls=StatuteFoundInUnit,
            items=statute.statutes_found,
        )
        return statute.id


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
    ```
    """

    conn: Connection
    path: Path = Field(default=DB_FOLDER)

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

    def add(self, row: DecisionRow) -> str | None:
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
            self.conn.add_record(
                kls=OpinionRow,
                item=op.dict(),
            )
            self.conn.add_records(
                kls=SegmentRow,
                items=list(op.dict() for op in op.segments),
            )

        return row.id

    @classmethod
    def restart(cls):
        config = cls.setup(reset=True)
        setup_pax(str(config.conn.path_to_db))
        config.build_tables()
