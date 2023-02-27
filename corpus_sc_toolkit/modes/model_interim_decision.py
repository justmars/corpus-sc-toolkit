import yaml
import datetime
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Self
from loguru import logger
from pydantic import Field, BaseModel
from sqlite_utils import Database
from dateutil.parser import parse

from ..meta import CourtComposition, DecisionCategory, get_cite_from_fields
from ..justice import CandidateJustice
from .resources import (
    DecisionFields,
    TEMP_FOLDER,
    origin,
    CLIENT,
    bucket_name,
    SQL_QUERY,
)


class InterimOpinion(BaseModel):
    id: str = Field(default=...)
    decision_id: str = Field(default=...)
    candidate: CandidateJustice = Field(exclude=True)
    title: str | None = Field(
        default=...,
        description=(
            "How is the opinion called, e.g. Ponencia, Concurring Opinion,"
            " Separate Opinion"
        ),
        col=str,
    )
    body: str = Field(
        default=...,
        title="Opinion Body",
        description="Text proper of the opinion.",
    )
    annex: str | None = Field(
        default=None,
        title="Opinion Annex",
        description="Annex portion of the opinion.",
    )
    pdf: str = Field(description="Downloadable link to the opinion pdf.")

    class Config:
        arbitrary_types_allowed = True

    @property
    def row(self) -> dict[str, str]:
        """Row to be used in OpinionRow table."""
        text = f"{self.body}\n----\n{self.annex}"
        base = self.dict(include={"id", "decision_id", "pdf", "title"})
        extended = {"justice_id": self.candidate.id, "text": text}
        return base | extended

    @property
    def ponencia_meta(self):
        """Used to return relevant details of the ponencia in `setup()`"""
        if self.title == "Ponencia":
            if self.candidate and self.candidate.detail:
                return self.candidate.detail._asdict()
        return None

    @classmethod
    def setup(cls, idx: str, db: Database, data: dict) -> dict | None:
        """Presumes existence of following keys:

        This will partially process the sql query defined in
        `/sql/limit_extract.sql`

        The required fields in `data`:

        1. `opinions` - i.e. a string made of `json_group_array`, `json_object` from sqlite query
        2. `date` - for determining the justice involved in the opinion/s
        """  # noqa: E501
        prerequisite = "id" in data and "date" in data and "opinions" in data
        if not prerequisite:
            return None

        opinions = []
        match_ponencia = {}
        keys = ["id", "title", "body", "annex"]
        for op in json.loads(data["opinions"]):
            opinion = cls(
                decision_id=idx,
                pdf=f"https://sc.judiciary.gov.ph{op['pdf']}",
                candidate=CandidateJustice(db, op.get("writer"), data["date"]),
                **{k: v for k, v in op.items() if k in keys},
            )
            opinions.append(opinion)
            if opinion.ponencia_meta:
                match_ponencia = opinion.ponencia_meta
        return {"opinions": opinions} | match_ponencia


class InterimDecision(DecisionFields):
    opinions: list[dict] = Field(default_factory=list)
    segments: list[dict] = Field(default_factory=list)

    @property
    def pdf_prefix(self) -> str | None:
        if not self.base_prefix or not self.docket_citation:
            return None

        if not self.is_pdf:
            logger.warning("Method limited to pdf-based files.")
            return None

        return f"{self.base_prefix}/pdf.yaml"

    def dump_pdf(self) -> Path:
        p = TEMP_FOLDER / "temp_pdf.yaml"
        p.unlink(missing_ok=True)
        with open(p, "w+") as f:
            yaml.safe_dump({"id": self.id} | self.dict(), f)
        return p

    def upload_pdf(self, override: bool = False) -> bool:
        loc = self.pdf_prefix
        if not loc:
            return False

        exist = CLIENT.get_object(Bucket=bucket_name, Key=loc)
        if not exist or (exist and override):
            origin.upload(file_like=self.dump_pdf(), loc=loc, args=self.meta)
            return True
        return False

    @classmethod
    def fetch(cls, db: Database) -> Iterator[Self]:
        """Given pdf-populated database, extract based on sql query defined in
        `/sql/limit_extract.sql`."""
        for row in db.execute_returning_dicts(SQL_QUERY):
            if not (cite := get_cite_from_fields(row)):
                logger.error(f"Bad citation in {row['id']=}")
                continue

            decision = cls(
                is_pdf=True,
                origin=row["id"],
                title=row["title"],
                description=cite.display,
                created=datetime.datetime.now().timestamp(),
                modified=datetime.datetime.now().timestamp(),
                date=parse(row["date"]).date(),
                date_scraped=parse(row["scraped"]).date(),
                citation=cite,
                composition=CourtComposition._setter(text=row["composition"]),
                emails=["bot@lawsql.com"],
                category=DecisionCategory.set_category(
                    row.get("category"), row.get("notice")
                ),
            )
            if not decision.id:
                logger.error(f"Undetected decision ID, see {cite=}")
                continue

            opx = InterimOpinion.setup(idx=decision.id, db=db, data=row)
            if not opx or not opx.get("opinions"):
                logger.error(f"No opinions detected in {decision.id=}")
                continue
            decision.raw_ponente = opx.get("raw_ponente", None)
            decision.per_curiam = opx.get("per_curiam", False)
            decision.justice_id = opx.get("justice_id", None)
            opinions = opx["opinions"]
            decision.opinions = [opinion.row for opinion in opinions]
            yield decision
