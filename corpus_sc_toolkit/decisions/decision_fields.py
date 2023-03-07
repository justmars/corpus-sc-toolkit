import datetime
from collections.abc import Iterator
from typing import Any, Self

from citation_utils import Citation
from loguru import logger
from pydantic import BaseModel, Field, root_validator

from ._resources import DECISION_BUCKET_NAME, DECISION_CLIENT, decision_storage
from .decision_opinions import DecisionOpinion
from .fields import CourtComposition, DecisionCategory


class DecisionFields(BaseModel):
    """
    A `Decision` relies on pre-processing various fields.

    This toolkit helps process some of those fields prior to insertion into a
    terminal database (even if they may previously originate from another
    third-party database.)

    Field | Type | Description
    :--:|:--:|:--
    id | str | Using the docket citation as identifier, uses `.` as dividing mechanism
    prefix | str | Location in cloud storage for saving / retrieving content delimited by `/`
    origin | str | Where decision was sourced from
    title | str | The case title, this can be classified into [tags][title-tags]
    description | str | The citation display
    date | datetime.date | The date the case was promulgated
    date_scraped | datetime.date | The date the case was scraped
    citation | optional[Citation] | The citation object
    composition | [CourtComposition][court-composition] | Whether the court sat en banc or in division
    category | [DecisionCategory][decision-category] | Whether the case decided was a decision or a resolution
    raw_ponente| optional[str] | Who decided case, if available
    justice_id | optional[int] | The [justice id][justice], if available
    per_curiam | bool. Defaults to False. | Whether case was decided per curiam
    is_pdf | bool. Defaults to False. | Whether case originated from a PDF file
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
        description="Get justice_id using `update_justice_ids.sql`.",
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
    emails: list[str] = Field(default_factory=list)
    opinions: list[DecisionOpinion] = Field(default_factory=list)

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
        """Metadata included as extra arguments to file uploaded."""
        if not self.citation or not self.citation.storage_prefix:
            return {}
        return {
            "ID": self.id,
            "Prefix": self.prefix,
            "Title": self.title,
            "Category": self.category,
            "Composition": self.composition,
            "Docket_Category": self.citation.docket_category,
            "Docket_ID": self.citation.docket_serial,
            "Docket_Date": self.date.isoformat(),
            "Report_Phil": self.citation.phil,
            "Report_Scra": self.citation.scra,
            "Report_Off_Gaz": self.citation.offg,
            "Has_PDF": self.is_pdf,
        }

    @classmethod
    def key_raw(cls, dated_prefix: str) -> str | None:
        """Is suffix `details.yaml` present in result of `cls.iter_dockets()`?"""
        key = f"{dated_prefix}details.yaml"
        try:
            DECISION_CLIENT.get_object(Bucket=DECISION_BUCKET_NAME, Key=key)
            return key
        except Exception:
            return None

    @classmethod
    def key_pdf(cls, dated_prefix: str) -> str | None:
        """Is suffix `pdf.yaml` present in result of `cls.iter_dockets()`?"""
        key = f"{dated_prefix}pdf.yaml"
        try:
            DECISION_CLIENT.get_object(Bucket=DECISION_BUCKET_NAME, Key=key)
            return key
        except Exception:
            return None

    def put_in_storage(self, suffix: str):
        """Puts Pydantic exported data dict to `details.yaml` or `pdf.yaml` in
        R2, depending on the value of `suffix`."""
        if suffix not in ("details.yaml", "pdf.yaml"):
            raise Exception("Invalid upload path.")
        remote_loc = f"{self.prefix}/{suffix}"
        temp_file = decision_storage.make_temp_yaml_path_from_data(
            self.dict(exclude_none=True)
        )
        args = decision_storage.set_extra_meta(self.storage_meta)
        logger.info(f"Uploading file to {remote_loc=}")
        decision_storage.upload(file_like=temp_file, loc=remote_loc, args=args)
        temp_file.unlink()

    @classmethod
    def get_from_storage(cls, prefix: str) -> Self:
        """Retrieves Pydantic exported data dict from either `details.yaml`, `pdf.yaml`
        in R2 (see extracted prefix from `key_pdf()` or `key_raw`()`) and instantiate
        the dict as a class instance."""
        if not prefix.endswith(("details.yaml", "pdf.yaml")):
            raise Exception("Bad path for DecisionFields base class.")

        data = decision_storage.restore_temp_yaml(prefix)
        if not data:
            raise Exception(f"Could not originate {prefix=}")

        logger.info(f"Retrieved file from {prefix=}")
        return cls(**data)
