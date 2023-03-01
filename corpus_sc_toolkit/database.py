import sys
from collections.abc import Iterator
from pathlib import Path

import yaml
from corpus_pax import Individual, setup_pax
from dotenv import find_dotenv, load_dotenv
from loguru import logger
from pydantic import BaseModel, Field
from pylts import ConfigS3
from sqlite_utils import Database
from sqlpyd import Connection

from .decision import (
    CitationRow,
    DecisionRow,
    OpinionRow,
    SegmentRow,
    TitleTagRow,
    VoteLine,
)
from .justice import Justice, get_justices_file
from .meta import extract_votelines, tags_from_title
from .modes import DOCKETS, YEARS, InterimDecision, RawDecision

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


class ConfigDecisions(BaseModel):
    """How to configure:

    ```py
        # set up initial PDF tables
        from corpus_sc_toolkit import Config
        config = ConfigDecisions.setup(reset=True)

        # add individuals / organizations to the database
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
        """
        Get the sqlite database from aws containing pdf tables via litestream.
        The database file becomes the focal point of the instance.
        """
        return cls(
            conn=Connection(
                DatabasePath=str(cls.get_pdf_db(reset=reset)),
                WAL=True,
            )
        )

    def build_tables(self) -> Database:
        """Create all the relevant tables involving a decision object."""
        logger.info("Ensure tables are created.")
        # Populate pax tables so that authors can be associated with decisions
        justices = yaml.safe_load(get_justices_file().read_bytes())
        self.conn.add_records(Justice, justices)
        self.conn.create_table(DecisionRow)
        self.conn.create_table(CitationRow)
        self.conn.create_table(OpinionRow)
        self.conn.create_table(VoteLine)
        self.conn.create_table(TitleTagRow)
        self.conn.create_table(SegmentRow)
        self.conn.db.index_foreign_keys()
        logger.info("Decision-based tables ready.")
        return self.conn.db

    def iter_decisions(
        self, dockets: list[str] = DOCKETS, years: tuple[int, int] = YEARS
    ) -> Iterator[DecisionRow]:
        """R2 uploaded content is formatted via:

        1. `RawDecision`: `details.yaml` variant SC e-library html content;
        2. `InterimDecision`: `pdf.yaml` variant SC links to PDF docs.

        Based on a filter from `dockets` and `years`, fetch from R2 storage either
        the `RawDecision` or the `InterimDecision`, with priority given to the former,
        i.e. if the `RawDecision` exists, use this; otherwise use `InterimDecision`.

        Args:
            db (Database): Will be used for `RawDecision.make()`
            dockets (list[str], optional): See `DecisionFields`. Defaults to DOCKETS.
            years (tuple[int, int], optional): See `DecisionFields`. Defaults to YEARS.

        Yields:
            Iterator[Self]: Unified decision item regardless of whether the source is
                a `details.yaml` file or a `pdf.yaml` file.
        """
        for docket_prefix in DecisionRow.iter_dockets(dockets, years):
            if key_raw := DecisionRow.key_raw(docket_prefix):
                r2_data = RawDecision.preget(key_raw)
                raw = RawDecision.make(r2_data=r2_data, db=self.conn.db)
                if raw and raw.prefix_id:
                    yield DecisionRow(**raw.dict(), id=raw.prefix_id)
            elif key_pdf := DecisionRow.key_pdf(docket_prefix):
                pdf = InterimDecision.get(key_pdf)
                if pdf.prefix_id:
                    yield DecisionRow(**pdf.dict(), id=pdf.prefix_id)

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
        for index, item in enumerate(config.iter_decisions()):
            logger.info(f"{item.id=}; {index=}")
            if decision_added := config.add_decision(item):
                logger.success(f"{decision_added=}")
            else:
                logger.error(f"{item.id=}")
