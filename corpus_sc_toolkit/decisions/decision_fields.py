import datetime
from typing import Self

from citation_utils import Citation
from loguru import logger
from pydantic import BaseModel, Field, root_validator

from ._resources import decision_storage
from .decision_opinions import DecisionOpinion
from .fields import CourtComposition, DecisionCategory


class DecisionFields(BaseModel):
    """
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
            "id": self.id,
            "prefix": self.prefix,
            "title": self.title,
            "category": self.category,
            "composition": self.composition,
            "docket_category": self.citation.docket_category,
            "docket_id": self.citation.docket_serial,
            "docket_date": self.date.isoformat(),
            "report_phil": self.citation.phil,
            "report_scra": self.citation.scra,
            "report_off_gaz": self.citation.offg,
            "has_pdf": self.is_pdf,
        }

    def put_in_storage(self, suffix: str):
        """Puts Pydantic exported data dict to `details.yaml` or `pdf.yaml` in
        R2, depending on the value of `suffix`."""

        # Set guard on suffix only
        if suffix not in ("details.yaml", "pdf.yaml"):
            raise Exception("Invalid upload path.")

        # Prepare instance values
        output_data = self.dict(exclude_none=True)
        remote_loc = f"{self.prefix}/{suffix}"
        temp_file = decision_storage.make_temp_yaml_path_from_data(output_data)
        args = decision_storage.set_extra_meta(self.storage_meta)

        # Put proper
        logger.info(f"Uploading file to {remote_loc=}")
        decision_storage.upload(file_like=temp_file, loc=remote_loc, args=args)
        temp_file.unlink()

    @classmethod
    def get_from_storage(cls, prefix: str) -> Self:
        """Retrieves Pydantic exported data dict from either `details.yaml`, `pdf.yaml`
        in R2 (see extracted prefix from `key_pdf()` or `key_raw`()`) and instantiate
        the dict as a class instance."""

        # Set guard on entire prefix
        if not prefix.endswith(("details.yaml", "pdf.yaml")):
            raise Exception("Bad path for DecisionFields base class.")

        # Get proper
        data = decision_storage.restore_temp_yaml(prefix)
        if not data:
            raise Exception(f"Could not originate {prefix=}")

        # Return instance
        logger.info(f"Retrieved file from {prefix=}")
        return cls(**data)
