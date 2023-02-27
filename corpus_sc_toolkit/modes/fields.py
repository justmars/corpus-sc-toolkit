import datetime
from citation_utils import Citation
from pydantic import BaseModel, Field

from typing import NamedTuple
from corpus_sc_toolkit.meta import (
    CourtComposition,
    DecisionCategory,
    DecisionSource,
)


class DecisionFields(BaseModel):
    """A Decision will rely on the previous processing of various fields.

    This toolkit helps process some of those fields prior to insertion into a
    terminal database (even if they may previously originate from another
    third-party database.)

    Field | Type | Description
    :--:|:--:|:--
    created | float | When was this model instantiated, for paths, this is when the file was actually made
    modified | float |  When was this model last modified, for paths, this is when the file was actually modified
    id | str | The [combination of various strings][set-decision-id-from-values] based on the source and citation, if available.
    origin | str | If `sc` source, this refers to the URL slug stem
    title | str | The case title, this can be classified into [tags][title-tags]
    description | str | The citation display
    date | datetime.date | The date the case was promulgated
    date_scraped | datetime.date | The date the case was scraped
    citation | optional[Citation] | The citation object
    composition | [CourtComposition][court-composition] | Whether the court sat en banc or in division
    category | [DecisionCategory][decision-category] | Whether the case decided was a decision or a resolution
    raw_ponente| optional[str] | Who decided the case, if available
    justice_id | optional[int] | The [justice id][justice], if available
    per_curiam | bool. Defaults to False. | Whether the case was decided per curiam
    is_pdf | bool. Defaults to False. | Whether the case originated from a PDF file
    fallo | optional[str] | Detected fallo / dispositive portion
    voting | optional[str] | Detected [voting line][vote-lines]
    emails | list[str] | Emails of authors
    """  # noqa: E501

    created: float
    modified: float
    id: str
    origin: str
    title: str
    description: str
    date: datetime.date
    date_scraped: datetime.date
    citation: Citation | None = None
    composition: CourtComposition
    category: DecisionCategory
    raw_ponente: str | None = None
    justice_id: int | None = None
    per_curiam: bool = False
    is_pdf: bool = False
    fallo: str | None = None
    voting: str | None = None
    emails: list[str] = Field(default_factory=list)

    class Config:
        use_enum_values = True

    def source(self) -> str:
        """See [DecisionSource][decision-source], may either be `sc` or `legacy`."""
        if self.date >= datetime.datetime(year=1996, month=1, day=1):
            return DecisionSource.sc
        return DecisionSource.legacy


class OpinionSegment(NamedTuple):
    id: str
    opinion_id: str
    decision_id: str
    position: str
    segment: str
    char_count: int

    @classmethod
    def set(
        cls,
        elements: list,
        opinion_id: str,
        text: str,
        position: str,
        decision_id: str,
    ):
        if all(elements):
            return cls(
                id="-".join(str(i) for i in elements),
                opinion_id=opinion_id,
                decision_id=decision_id,
                position=position,
                segment=text,
                char_count=len(text),
            )

    @classmethod
    def set_from_txt(
        cls, decision_id: str, opinion_id: str, opinion_text: str
    ):
        from corpus_sc_toolkit import segmentize

        for extract in segmentize(opinion_text):
            yield cls(
                id=f"{opinion_id}-{extract['position']}",
                decision_id=decision_id,
                opinion_id=opinion_id,
                **extract,
            )


class DecisionOpinion(NamedTuple):
    id: str
    decision_id: str
    title: str
    text: str
    tags: list[str]
    pdf: str | None = None
    remark: str | None = None
    concurs: list[dict] | None = None
    justice_id: int | None = None

    @property
    def segments(self) -> list[OpinionSegment]:
        return list(
            OpinionSegment.set_from_txt(
                decision_id=self.decision_id,
                opinion_id=self.id,
                opinion_text=self.text,
            )
        )
