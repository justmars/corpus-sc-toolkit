import abc
from collections.abc import Iterator
from sqlite3 import IntegrityError

import yaml
from corpus_pax import Individual
from loguru import logger
from pydantic import BaseModel
from sqlite_utils import Database
from sqlpyd import Connection
from start_sdk import StorageUtils

from .decisions import (
    CitationInOpinion,
    CitationRow,
    DecisionRow,
    Justice,
    OpinionRow,
    OpinionTitleTagRow,
    SegmentRow,
    StatuteInOpinion,
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


class StorageToDatabaseConfiguration(BaseModel, abc.ABC):
    conn: Connection
    storage: StorageUtils

    @abc.abstractmethod
    def set_tables(self) -> None:
        """Prep tables for data entry."""
        raise NotImplementedError

    @abc.abstractmethod
    def add_row(self) -> None:
        """Implies organization of table entries from a retrieved storage instance."""
        raise NotImplementedError

    @abc.abstractmethod
    def add_rows(self) -> None:
        """Using storage items converted into rows add each to database."""
        raise NotImplementedError


class ConfigStatutes(StorageToDatabaseConfiguration):
    def set_tables(self):
        self.conn.create_table(StatuteRow)
        self.conn.create_table(StatuteTitleRow)
        self.conn.create_table(StatuteUnitSearch)
        self.conn.create_table(StatuteMaterialPath)
        self.conn.create_table(StatuteFoundInUnit)
        self.conn.db.index_foreign_keys()
        logger.info("Statute-based tables ready.")
        return self.conn.db

    def existing_rows(self) -> Iterator[str]:
        table = self.conn.db[StatuteRow.__tablename__]
        for row in table.rows_where(select="id"):
            yield row["id"]

    def add_row(self, statute: Statute):
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

        for statute_title in statute.titles:
            statute_title.statute_id = statute.id  # see TODO in Statute
            self.conn.add_record(
                kls=StatuteTitleRow,
                item=statute_title.dict(),
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
            kls=StatuteFoundInUnit,
            items=statute.statutes_found,
        )
        return statute.id

    def add_rows(self):
        self.set_tables()
        if statute_prefixes := self.storage.all_items():
            for prefix in statute_prefixes:
                if prefix["Key"].endswith("details.yaml"):
                    try:
                        row = self.add_row(Statute.get(prefix["Key"]))
                        logger.success(f"Added: {row=}")
                    except Exception as e:
                        logger.error(f"Bad {prefix['key']}; {e=}")


class ConfigDecisions(StorageToDatabaseConfiguration):
    def set_tables(self) -> Database:
        logger.info("Ensure tables are created.")
        try:
            justices = yaml.safe_load(get_justices_file().read_bytes())
            self.conn.add_records(Justice, justices)
        except IntegrityError:
            ...  # already existing table because of prior addition
        self.conn.create_table(DecisionRow)
        self.conn.create_table(OpinionRow)
        self.conn.create_table(CitationRow)
        self.conn.create_table(VoteLine)
        self.conn.create_table(TitleTagRow)
        self.conn.create_table(SegmentRow)
        self.conn.db.index_foreign_keys()
        logger.info("Decision-based tables ready.")
        return self.conn.db

    def existing_rows(self) -> Iterator[str]:
        table = self.conn.db[DecisionRow.__tablename__]
        for row in table.rows_where(select="id"):
            yield row["id"]

    def add_row(self, row: DecisionRow) -> str | None:
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
            self.conn.add_record(
                kls=CitationRow,
                item=row.citation_fk,
            )
        if row.voting:
            self.conn.add_records(
                kls=VoteLine,
                items=extract_votelines(
                    decision_pk=added.last_pk,
                    text=row.voting,
                ),
            )
        if row.title:
            self.conn.add_records(
                kls=TitleTagRow,
                items=tags_from_title(
                    decision_pk=added.last_pk,
                    text=row.title,
                ),
            )

        for op in row.opinions:
            self.conn.add_record(
                kls=OpinionRow,
                item=op.dict(),
            )
            self.conn.add_cleaned_records(
                kls=SegmentRow,
                items=op.segments,
            )
            base_op = {"opinion_id": op.id, "decision_id": op.decision_id}
            if op.tags:
                self.conn.add_records(
                    kls=OpinionTitleTagRow,
                    items=[base_op | {"label": tag} for tag in op.tags],
                )
            if op.statutes:
                self.conn.add_records(
                    kls=StatuteInOpinion,
                    items=[base_op | stat.dict() for stat in op.statutes],
                )
            self.conn.add_records(
                kls=CitationInOpinion,
                items=[base_op | cite.dict() for cite in op.citations],
            )
        return row.id

    def add_rows(self):
        self.set_tables()
        if decision_prefixes := self.storage.all_items():
            for item in decision_prefixes:
                if row := DecisionRow.from_key(item["Key"]):
                    if row_added := self.add_row(row):
                        logger.success(f"{row_added=}")
