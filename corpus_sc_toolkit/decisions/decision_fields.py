import datetime
from collections.abc import Iterator
from typing import Any

from citation_utils import Citation
from loguru import logger
from pydantic import BaseModel, Field, root_validator

from .._utils import get_from_prefix
from ._resources import (
    DECISION_BUCKET_NAME,
    DECISION_CLIENT,
    DETAILS_FILE,
    DOCKETS,
    PDF_FILE,
    YEARS,
)
from .decision_substructures import DecisionOpinion
from .fields import CourtComposition, DecisionCategory


class DecisionFields(BaseModel):
    """
    A `Decision` relies on pre-processing various fields.

    This toolkit helps process some of those fields prior to insertion into a
    terminal database (even if they may previously originate from another
    third-party database.)

    Field | Type | Description
    :--:|:--:|:--
    id | str | The [combination of various strings][set-decision-id-from-values] based on the source and citation, if available.
    origin | str | Where the decision was sourced from
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
    opinions | list[DecisionOpinion] | [Opinion structures][decision opinions] which can be further [subdivided into segments][opinion segments]
    """  # noqa: E501

    id: str = Field(col=str)
    prefix: str = Field(col=str)
    citation: Citation | None = Field(default=None)  # overriden in decision.py
    origin: str = Field(col=str, index=True)
    title: str = Field(col=str, index=True, fts=True)
    description: str = Field(col=str, index=True, fts=True)
    date: datetime.date = Field(col=datetime.date, index=True)
    date_scraped: datetime.date = Field(col=datetime.date, index=True)
    composition: CourtComposition = Field(default=None, col=str, index=True)
    category: DecisionCategory = Field(default=None, col=str, index=True)
    raw_ponente: str | None = Field(
        default=None,
        title="Ponente",
        description="Lowercase and be suitable for matching a justice id.",
        col=str,
        index=True,
    )
    justice_id: int | None = Field(
        default=None,
        title="Justice ID",
        description=(
            "Determine appropriate justice_id using `update_justice_ids.sql`."
        ),
        col=int,
        index=True,
    )
    per_curiam: bool = Field(
        default=False,
        title="Is Per Curiam",
        description="If true, decision was penned anonymously.",
        col=bool,
        index=True,
    )
    is_pdf: bool | None = Field(default=False, col=bool, index=True)
    fallo: str | None = Field(default=None, col=str, index=True, fts=True)
    voting: str | None = Field(default=None, col=str, index=True, fts=True)
    emails: list[str] = Field(default_factory=list, exclude=True)
    opinions: list[DecisionOpinion] = Field(default_factory=list, exclude=True)

    @root_validator()
    def citation_date_is_object_date(cls, values):
        cite, date = values.get("citation"), values.get("date")
        if cite and cite.docket_date:
            if cite.docket_date != date:
                msg = f"Inconsistent {cite.docket_date=} vs. {date=};"
                logger.error(msg)
                raise ValueError(msg)
        return values

    class Config:
        use_enum_values = True

    @property
    def storage_meta(self):
        """When uploading to R2, the metadata can be included as extra arguments to
        the file."""
        if not self.citation or not self.citation.storage_prefix:
            return {}
        return {
            "Title": self.title,
            "Category": self.category,
            "Composition": self.composition,
            "Docket_Category": self.citation.docket_category,
            "Docket_ID": self.citation.docket_serial,
            "Docket_Date": self.date.isoformat(),
            "Report_Phil": self.citation.phil,
            "Report_Scra": self.citation.scra,
            "Report_Off_Gaz": self.citation.offg,
        }

    @classmethod
    def iter_docket_dates(
        cls, dockets: list[str] = DOCKETS, years: tuple[int, int] = YEARS
    ) -> Iterator[str]:
        """Results in the following prefix format: `<docket>/<year>/<month>`
        in ascending order."""
        for docket in dockets:
            cnt_year, end_year = years[0], years[1]
            while cnt_year <= end_year:
                for month in range(1, 13):
                    yield f"{docket}/{cnt_year}/{month}/"
                cnt_year += 1

    @classmethod
    def iter_docket_date_serials(
        cls, dockets: list[str] = DOCKETS, years: tuple[int, int] = YEARS
    ) -> Iterator[dict[str, Any]]:
        """Results in a collection based on the prefix format:
        `<docket>/<year>/<month>/<serial>/` in ascending order. Each item in the
        collection is a dict which will contain a `CommonPrefixes` key."""
        for prefix in cls.iter_docket_dates(dockets, years):
            yield DECISION_CLIENT.list_objects_v2(
                Bucket=DECISION_BUCKET_NAME, Delimiter="/", Prefix=prefix
            )

    @classmethod
    def iter_dockets(
        cls, dockets: list[str] = DOCKETS, years: tuple[int, int] = YEARS
    ) -> Iterator[str]:
        """For each item in the collection from `cls.iter_docket_dates()`, produce
        unique docket keys."""
        for collection in cls.iter_docket_date_serials(dockets, years):
            if collection.get("CommonPrefixes", None):
                for docket in collection["CommonPrefixes"]:
                    yield docket["Prefix"]
            else:
                logger.warning(f"Empty {collection=}")

    @classmethod
    def key_raw(cls, dated_prefix: str) -> str | None:
        """Is suffix `details.yaml` present in result of `cls.iter_dockets()`?"""
        target_key = f"{dated_prefix}{DETAILS_FILE}"
        res = get_from_prefix(
            client=DECISION_CLIENT,
            bucket_name=DECISION_BUCKET_NAME,
            key=target_key,
        )
        return target_key if res else None

    @classmethod
    def key_pdf(cls, dated_prefix: str) -> str | None:
        """Is suffix `pdf.yaml` present in result of `cls.iter_dockets()`?"""
        target_key = f"{dated_prefix}{PDF_FILE}"
        res = get_from_prefix(
            client=DECISION_CLIENT,
            bucket_name=DECISION_BUCKET_NAME,
            key=target_key,
        )
        return target_key if res else None
