from loguru import logger
from citation_utils import Citation
from pydantic import Field, root_validator
from sqlpyd import TableConfig
from .justice import Justice
from .modes import DecisionFields, DecisionOpinion


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
    id: str = Field(col=str)
    citation: Citation = Field(exclude=True)
    opinions: list[DecisionOpinion] = Field(default_factory=list, exclude=True)

    @root_validator()
    def citation_date_is_object_date(cls, values):
        cite, date = values.get("citation"), values.get("date")
        if cite.docket_date:
            if cite.docket_date != date:
                msg = f"Inconsistent {cite.docket_date=} vs. {date=};"
                logger.error(msg)
                raise ValueError(msg)
        return values

    class Config:
        use_enum_values = True

    @property
    def citation_fk(self) -> dict:
        return self.citation.dict() | {"decision_id": self.id}


DECISION_ID = Field(
    default=...,
    title="Decision ID",
    description=(
        "Foreign key used by other tables referencing the Decision table."
    ),
    col=str,
    fk=(DecisionRow.__tablename__, "id"),
)


class CitationRow(Citation, TableConfig):
    __prefix__ = "sc"
    __tablename__ = "citations"
    __indexes__ = [
        ["id", "decision_id"],
        ["docket_category", "docket_serial", "docket_date"],
        ["scra", "phil", "offg", "docket"],
    ]
    decision_id: str = DECISION_ID


class VoteLine(TableConfig):
    __prefix__ = "sc"
    __tablename__ = "votelines"
    __indexes__ = [["id", "decision_id"]]
    decision_id: str = DECISION_ID
    text: str = Field(
        ...,
        title="Voteline Text",
        description=(
            "Each decision may contain a vote line, e.g. a summary of which"
            " justice voted for the main opinion and those who dissented, etc."
        ),
        col=str,
        index=True,
    )


class TitleTagRow(TableConfig):
    __prefix__ = "sc"
    __tablename__ = "titletags"
    decision_id: str = DECISION_ID
    tag: str = Field(..., col=str, index=True)


class OpinionRow(TableConfig):
    __prefix__ = "sc"
    __tablename__ = "opinions"
    __indexes__ = [
        ["id", "title"],
        ["id", "justice_id"],
        ["id", "decision_id"],
        ["decision_id", "title"],
    ]
    decision_id: str = DECISION_ID
    id: str = Field(
        ...,
        description=(
            "The opinion pk is based on combining the decision_id with the"
            " justice_id"
        ),
        col=str,
    )
    pdf: str | None = Field(
        default=None,
        description=(
            "The opinion pdf is the url that links to the downloadable PDF, if"
            " it exists"
        ),
        col=str,
    )
    title: str | None = Field(
        ...,
        description=(
            "How is the opinion called, e.g. Ponencia, Concurring Opinion,"
            " Separate Opinion"
        ),
        col=str,
    )
    tags: list[str] | None = Field(
        default=None,
        description="e.g. main, dissenting, concurring, separate",
    )
    justice_id: int | None = Field(
        default=None,
        description=(
            "The writer of the opinion; when not supplied could mean a Per"
            " Curiam opinion, or unable to detect the proper justice."
        ),
        col=int,
        index=True,
        fk=(Justice.__tablename__, "id"),
    )
    remark: str | None = Field(
        default=None,
        description=(
            "Short description of the opinion, when available, i.e. 'I reserve"
            " my right, etc.', 'On leave.', etc."
        ),
        col=str,
        fts=True,
    )
    concurs: list[dict] | None = Field(default=None)
    text: str = Field(
        ...,
        description=(
            "Text proper of the opinion (should ideally be in markdown format)"
        ),
        col=str,
        fts=True,
    )


class SegmentRow(TableConfig):
    __prefix__ = "sc"
    __tablename__ = "segments"
    __indexes__ = [["opinion_id", "decision_id"]]
    id: str = Field(..., col=str)
    decision_id: str = DECISION_ID
    opinion_id: str = Field(..., col=str, fk=(OpinionRow.__tablename__, "id"))
    position: str = Field(
        ...,
        title="Relative Position",
        description=(
            "The line number of the text as stripped from its markdown source."
        ),
        col=int,
        index=True,
    )
    char_count: int = Field(
        ...,
        title="Character Count",
        description=(
            "The number of characters of the text makes it easier to discover"
            " patterns."
        ),
        col=int,
        index=True,
    )
    segment: str = Field(
        ...,
        title="Body Segment",
        description=(
            "A partial text fragment of an opinion, exclusive of footnotes."
        ),
        col=str,
        fts=True,
    )
