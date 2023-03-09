import abc
from collections.abc import Iterator
from sqlite3 import IntegrityError
from typing import Self

import yaml
from citation_utils import Citation
from corpus_pax import Individual
from loguru import logger
from pydantic import BaseModel, Field
from sqlite_utils import Database
from sqlpyd import TableConfig
from statute_trees import MentionedStatute

from corpus_sc_toolkit.store import StorageToDatabaseConfiguration

from .decision_fields import DecisionFields
from .decision_fields_via_html import DETAILS_KEY, DecisionHTML
from .decision_fields_via_pdf import PDF_KEY, DecisionPDF
from .decision_opinion_segments import OpinionSegment
from .decision_opinions import DecisionOpinion, OpinionTag
from .fields import extract_votelines, tags_from_title
from .justice import Justice, get_justices_file


class DecisionRow(DecisionFields, TableConfig):
    """R2 uploaded content is formatted via:

        Variant | Suffix | Source
        :--:|:--:|:--
        `DecisionHTML | suffixed `/details.yaml` | SC e-library html
        `DecisionPDF` | suffixed `/pdf.yaml` | SC PDFs from main site

    When these are converted back into python, we serialize through this model
    which contains the same fields as `DecisionHTML` and  `DecisionPDF`,
    with modifications to: `citation`, `emails`, and `opinions`.

    The assumption for the modifications is that is that serialization will
    lead to database entry and the fields modified will have their own tables
    separate from the main decision.
    """

    __prefix__ = "sc"
    __tablename__ = "decisions"
    __indexes__ = [
        ["date", "justice_id", "raw_ponente", "per_curiam"],
        ["origin", "date"],
        ["category", "composition"],
        ["id", "justice_id"],
        ["per_curiam", "raw_ponente"],
    ]
    citation: Citation = Field(default=..., exclude=True)
    emails: list[str] = Field(default_factory=list, exclude=True)
    opinions: list[DecisionOpinion] = Field(default_factory=list, exclude=True)

    @property
    def citation_fk(self) -> dict:
        return self.citation.dict() | {"decision_id": self.id}

    @classmethod
    def from_key(cls, key: str) -> Self | None:
        """Expects full key inclusive of whether `details.yaml` / `pdf.yaml`
        to retrieve an instance of `DecisionRow`, if available in r2."""
        if key.endswith(DETAILS_KEY):
            return cls(**DecisionHTML.get_from_storage(key).dict())
        elif key.endswith(PDF_KEY):
            return cls(**DecisionPDF.get_from_storage(key).dict())
        return None

    @classmethod
    def from_prefix(cls, prefix: str) -> Self | None:
        """Add one of two keys `details.yaml` / `pdf.yaml`" to the incomplete prefix
        to get the `DecisionRow`, if available in r2."""
        if obj := DecisionHTML.get_from_storage(f"{prefix}/{DETAILS_KEY}"):
            return cls(**obj.dict())
        elif obj := DecisionPDF.get_from_storage(f"{prefix}/{PDF_KEY}"):
            return cls(**obj.dict())
        return None


class DecisionComponent(BaseModel, abc.ABC):
    """Reusable abstract class referencing the Decision row."""

    __prefix__ = "sc"
    decision_id: str = Field(
        default=...,
        title="Decision ID",
        description="Foreign key to reference Decisions.",
        col=str,
        fk=(DecisionRow.__tablename__, "id"),
    )


class CitationRow(DecisionComponent, Citation, TableConfig):
    """How each decision is identified."""

    __tablename__ = "citations"
    __indexes__ = [
        ["id", "decision_id"],
        ["docket_category", "docket_serial", "docket_date"],
        ["scra", "phil", "offg", "docket"],
    ]


class VoteLine(DecisionComponent, TableConfig):
    """Each decision may contain a vote line, e.g. a summary of which
    justice voted for the main opinion and those who dissented, etc."""

    __tablename__ = "votelines"
    __indexes__ = [["id", "decision_id"]]
    text: str = Field(..., title="Voteline Text", col=str, index=True)


class TitleTagRow(DecisionComponent, TableConfig):
    """Enables some classifications based on the title of the decision."""

    __tablename__ = "titletags"
    tag: str = Field(..., col=str, index=True)


class OpinionRow(DecisionComponent, DecisionOpinion, TableConfig):
    """Component opinion of a decision."""

    __tablename__ = "opinions"
    __indexes__ = [
        ["id", "title"],
        ["id", "justice_id"],
        ["id", "decision_id"],
        ["decision_id", "title"],
    ]
    justice_id: int | None = Field(
        default=None,
        title="Justice ID",
        description="If empty, a Per Curiam opinion or unable to detect ID.",
        col=int,
        fk=(Justice.__tablename__, "id"),
    )
    tags: list[OpinionTag] = Field(exclude=True)
    statutes: list[MentionedStatute] = Field(exclude=True)
    segments: list[OpinionSegment] = Field(exclude=True)
    citations: list[Citation] = Field(exclude=True)


class OpinionComponent(DecisionComponent, abc.ABC):
    """Reusable abstract class referencing the Opinion row."""

    opinion_id: str = Field(
        default=...,
        title="Opinion Id",
        col=str,
        fk=(OpinionRow.__tablename__, "id"),
    )


class OpinionTitleTagRow(OpinionComponent, TableConfig):
    """Each opinion's title can have tags."""

    __tablename__ = "opinion_tags"
    __indexes__ = [
        ["opinion_id", "decision_id"],
        ["opinion_id", "label"],
    ]

    label: OpinionTag = Field(col=str, index=True)


class SegmentRow(OpinionComponent, OpinionSegment, TableConfig):
    """Each opinion can be divided into segments."""

    __tablename__ = "opinion_segments"
    __indexes__ = [["opinion_id", "decision_id"]]


class StatuteInOpinion(OpinionComponent, MentionedStatute, TableConfig):
    """Each opinion can contain references of statutes."""

    __tablename__ = "opinion_statutes"
    __indexes__ = [
        ["opinion_id", "decision_id"],
        ["statute_category", "statute_serial_id"],
    ]


class CitationInOpinion(OpinionComponent, Citation, TableConfig):
    """Each opinion can contain references of citations."""

    __tablename__ = "opinion_citations"
    __indexes__ = [
        ["opinion_id", "decision_id"],
        ["docket_category", "docket_serial", "docket_date"],
        ["scra", "phil", "offg", "docket"],
    ]


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

    def get_db_ids(self) -> Iterator[str]:
        table = self.conn.db[DecisionRow.__tablename__]
        for row in table.rows_where(select="id"):
            yield row["id"]

    def get_r2_ids(self) -> Iterator[str]:
        if objs := self.storage.all_items():
            detail_keys = [  # Get unique prefixes containing details
                detail["Key"].removesuffix(f"/{DETAILS_KEY}")
                for detail in self.storage.filter_content(DETAILS_KEY, objs)
            ]
            pdf_keys = [  # Get unique suffixes containing pdfs
                pdf["Key"].removesuffix(f"/{PDF_KEY}")
                for pdf in self.storage.filter_content(PDF_KEY, objs)
            ]
            for key in set(detail_keys + pdf_keys):
                yield key.replace("/", ".")

    def add_missing_r2_ids(self):
        r2_ids = set(self.get_r2_ids())
        db_ids = set(self.get_db_ids())
        for id in r2_ids.difference(db_ids):
            key = id.replace(".", "/")
            try:
                if row := DecisionRow.from_prefix(key):
                    if added := self.add_row(row):
                        logger.success(f"Added: {id=} {added=}")
            except Exception as e:
                logger.error(f"Bad {id}; {e=}")
