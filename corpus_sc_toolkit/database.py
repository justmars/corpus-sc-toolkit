from collections.abc import Iterator
from pathlib import Path

import yaml
from corpus_pax import Individual, setup_pax
from loguru import logger
from pydantic import BaseSettings, Field
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


class ConfigDecisions(BaseSettings):
    conn: Connection
    path: Path = Field(default=DB_FOLDER)

    @classmethod
    def get_pdf_db(cls) -> Path:
        src = "s3://corpus-pdf/db"
        logger.info(f"Restore from {src=} to {DB_FOLDER=}")
        stream = ConfigS3(s3=src, folder=DB_FOLDER)
        if stream.dbpath.exists():
            return stream.dbpath
        return stream.restore()

    @classmethod
    def setup(cls):
        dbpath = str(cls.get_pdf_db())
        conn = Connection(DatabasePath=dbpath, WAL=True)
        return cls(conn=conn)

    def build_tables(self) -> Database:
        """Create all the relevant tables involving a decision object."""
        logger.info(f"Ensure tables in {self.conn.db=}")
        justices = yaml.safe_load(get_justices_file().read_bytes())
        self.conn.add_records(Justice, justices)
        self.conn.create_table(DecisionRow)
        self.conn.create_table(CitationRow)
        self.conn.create_table(OpinionRow)
        self.conn.create_table(VoteLine)
        self.conn.create_table(TitleTagRow)
        self.conn.create_table(SegmentRow)
        self.conn.db.index_foreign_keys()
        return self.conn.db

    def reset(self) -> Connection:
        if self.conn.path_to_db:
            logger.info(f"Deleting {self.conn.path_to_db=}")
            self.conn.path_to_db.unlink()
        self.get_pdf_db()
        setup_pax(db_path=str(self.conn.path_to_db))
        self.build_tables()
        return self.conn

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
                yield DecisionRow(**InterimDecision.get(key_pdf).dict())

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
