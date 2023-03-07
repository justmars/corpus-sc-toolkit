import abc
from collections.abc import Iterator
from typing import Self

from citation_utils import Citation
from pydantic import BaseModel, Field
from sqlite_utils.db import Database
from sqlpyd import TableConfig

from ._resources import DOCKETS, YEARS, decision_storage
from .decision_fields import DecisionFields
from .decision_substructures import DecisionOpinion, OpinionSegment
from .interim import InterimDecision
from .justice import Justice
from .raw import RawDecision


class DecisionRow(DecisionFields, TableConfig):
    """The citation in a `DecisionRow` overrides `DecisionFields`
    since the row implies a valid citation already exists and is uploaded in R2.
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
    id: str = Field(col=str, description="Constructed in DecisionFields.")
    citation: Citation = Field(default=..., exclude=True)

    @property
    def citation_fk(self) -> dict:
        return self.citation.dict() | {"decision_id": self.id}

    @classmethod
    def from_cloud_storage(
        cls,
        db: Database,
        dockets: list[str] = DOCKETS,
        years: tuple[int, int] = YEARS,
    ) -> Iterator[Self]:
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
        for docket_prefix in cls.iter_dockets(dockets, years):
            if raw_k := cls.key_raw(docket_prefix):
                if rx := decision_storage.restore_temp_yaml(yaml_suffix=raw_k):
                    if raw := RawDecision.make_from_storage(r2_data=rx, db=db):
                        yield cls(**raw.dict())
            elif key_pdf := cls.key_pdf(docket_prefix):
                if pdf := InterimDecision.get(key_pdf):
                    yield cls(**pdf.dict())


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


class SegmentRow(DecisionComponent, OpinionSegment, TableConfig):
    """Component element of an opinion of a decision."""

    __tablename__ = "segments"
    __indexes__ = [["opinion_id", "decision_id"]]
    opinion_id: str = Field(
        default=...,
        title="Opinion Id",
        col=str,
        fk=(OpinionRow.__tablename__, "id"),
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
