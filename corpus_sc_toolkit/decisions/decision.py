import abc
from typing import Self

from citation_utils import Citation
from pydantic import BaseModel, Field
from sqlpyd import TableConfig

from .decision_fields import DecisionFields
from .decision_fields_via_html import DecisionHTML
from .decision_fields_via_pdf import DecisionPDF
from .decision_opinion_segments import OpinionSegment
from .decision_opinions import DecisionOpinion
from .justice import Justice


class DecisionRow(DecisionFields, TableConfig):
    __prefix__ = "sc"
    __tablename__ = "decisions"
    __indexes__ = [
        ["date", "justice_id", "raw_ponente", "per_curiam"],
        ["origin", "date"],
        ["category", "composition"],
        ["id", "justice_id"],
        ["per_curiam", "raw_ponente"],
    ]
    # overriden: citation, emails, opinions
    citation: Citation = Field(default=..., exclude=True)
    emails: list[str] = Field(default_factory=list, exclude=True)
    opinions: list[DecisionOpinion] = Field(default_factory=list, exclude=True)

    @property
    def citation_fk(self) -> dict:
        return self.citation.dict() | {"decision_id": self.id}

    @classmethod
    def from_cloud_storage(cls, docket_prefix: str) -> Self | None:
        """R2 uploaded content is formatted via:

        Variant | Suffix | Source
        :--:|:--:|:--"
        `DecisionHTML | suffixed `/details.yaml` | SC e-library html
        `DecisionPDF` | suffixed `/pdf.yaml` | SC PDFs from main site

        Args:
            docket_prefix (str): Should end in .yaml

        Returns:
            Self | None:  If found, prioritize `DecisionHTML` then `DecisionPDF`.
        """
        if key_html := cls.key_raw(docket_prefix):
            if html := DecisionHTML.get_from_storage(key_html):
                return cls(**html.dict())
        elif key_pdf := cls.key_pdf(docket_prefix):
            if pdf := DecisionPDF.get_from_storage(key_pdf):
                return cls(**pdf.dict())
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
