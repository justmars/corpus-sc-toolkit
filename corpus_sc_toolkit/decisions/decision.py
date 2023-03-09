import abc
from typing import Self

from citation_utils import Citation
from pydantic import BaseModel, Field
from sqlpyd import TableConfig
from statute_trees import MentionedStatute

from .decision_fields import DecisionFields
from .decision_fields_via_html import DETAILS_KEY, DecisionHTML
from .decision_fields_via_pdf import PDF_KEY, DecisionPDF
from .decision_opinion_segments import OpinionSegment
from .decision_opinions import DecisionOpinion, OpinionTag
from .justice import Justice


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
        if key.endswith(DETAILS_KEY):
            return cls(**DecisionHTML.get_from_storage(key).dict())
        elif key.endswith(PDF_KEY):
            return cls(**DecisionPDF.get_from_storage(key).dict())
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
    __indexes__ = [["opinion_id", "tag_label"]]

    tag_label: OpinionTag = Field(col=str, index=True)


class SegmentRow(OpinionComponent, OpinionSegment, TableConfig):
    """Each opinion can be divided into segments."""

    __tablename__ = "segments"
    __indexes__ = [["opinion_id", "decision_id"]]


class StatuteInOpinion(OpinionComponent, MentionedStatute, TableConfig):
    """Each opinion can contain references of statutes."""

    __tablename__ = "opinion_statutes"
    __indexes__ = [["opinion_id", "decision_id"]]


class CitationInOpinion(OpinionComponent, Citation, TableConfig):
    """Each opinion can contain references of citations."""

    __tablename__ = "opinion_citations"
    __indexes__ = [["opinion_id", "decision_id"]]
