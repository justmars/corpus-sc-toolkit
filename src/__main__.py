import json
import re
from collections.abc import Iterator
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Self

import yaml
from citation_docket import Docket, extract_dockets
from dateutil.parser import parse
from loguru import logger
from pydantic import BaseModel, Field
from sqlite_utils import Database
from unidecode import unidecode

from .clean import Notice

CATEGORY_START_DECISION = re.compile(r"d\s*e\s*c", re.I)
CATEGORY_START_RESOLUTION = re.compile(r"r\s*e\s*s", re.I)

COMPOSITION_START_DIVISION = re.compile(r"div", re.I)
COMPOSITION_START_ENBANC = re.compile(r"en", re.I)

SC_BASE_URL = "https://sc.judiciary.gov.ph"
PDF_DB_PATH: Path = Path().cwd() / "pdf.db"
TARGET_FOLDER: Path = Path().home() / "code" / "corpus" / "decisions" / "sc"


class DecisionCategory(str, Enum):
    decision = "Decision"
    resolution = "Resolution"
    other = "Unspecified"

    @classmethod
    def _setter(cls, text: str | None):
        if text:
            if CATEGORY_START_DECISION.search(text):
                return cls.decision
            elif CATEGORY_START_RESOLUTION.search(text):
                return cls.resolution
        return cls.other

    @classmethod
    def set_category(cls, category: str | None = None, notice: int | None = 0):
        if notice:
            return cls.resolution
        if category:
            cls._setter(category)
        return cls.other


class CourtComposition(str, Enum):
    enbanc = "En Banc"
    division = "Division"
    other = "Unspecified"

    @classmethod
    def _setter(cls, text: str | None):
        if text:
            if COMPOSITION_START_DIVISION.search(text):
                return cls.division
            elif COMPOSITION_START_ENBANC.search(text):
                return cls.enbanc
        return cls.other


class ExtractSegmentPDF(BaseModel):
    id: str = Field(...)
    opinion_id: str = Field(...)
    decision_id: int = Field(...)
    position: str = Field(...)
    segment: str = Field(...)
    char_count: int = Field(...)


class ExtractOpinionPDF(BaseModel):
    id: str = Field(...)
    decision_id: int = Field(...)
    pdf: str
    justice_label: str | None = Field(
        None,
        description=(
            "The writer of the opinion; when not supplied could mean a Per"
            " Curiam opinion, or unable to detect the proper justice."
        ),
    )
    title: str | None = Field(
        ...,
        description=(
            "How is the opinion called, e.g. Ponencia, Concurring Opinion,"
            " Separate Opinion"
        ),
        col=str,
    )
    body: str = Field(..., description="Text proper of the opinion.")
    annex: str | None = Field(
        default=None, description="Annex portion of the opinion."
    )
    segments: list[ExtractSegmentPDF] = Field(default_factory=list)

    def set_segments(self, path: Path = PDF_DB_PATH):
        db = Database(path)
        if self.title in ["Ponencia", "Notice"]:  # see decision_list.sql
            for row in db["pre_tbl_decision_segment"].rows_where(
                "decision_id = ? and length(text) > 10", (self.decision_id,)
            ):
                rowid, page_num = (
                    row.get("id"),
                    row.get("page_num"),
                )
                elements: list = [rowid, page_num, self.decision_id]
                if all(elements):
                    self.segments.append(
                        ExtractSegmentPDF(
                            id="-".join(str(i) for i in elements),
                            opinion_id=f"main-{self.decision_id}",
                            decision_id=self.decision_id,
                            position=f"{rowid}-{page_num}",
                            segment=row["text"],
                            char_count=len(row["text"]),
                        )
                    )
        else:
            for row in db["pre_tbl_opinion_segment"].rows_where(
                "opinion_id = ? and length(text) > 10", (self.id)
            ):
                rowid, page_num, opinion_id = (
                    row.get("id"),
                    row.get("page_num"),
                    row.get("opinion_id"),
                )
                elements: list = [rowid, page_num, opinion_id]
                if all(elements):
                    self.segments.append(
                        ExtractSegmentPDF(
                            id="-".join(str(i) for i in elements),
                            opinion_id=f"{str(self.decision_id)}-{opinion_id}",
                            decision_id=self.decision_id,
                            position=f"{rowid}-{page_num}",
                            segment=row["text"],
                            char_count=len(row["text"]),
                        )
                    )


class ExtractDecisionPDF(BaseModel):
    id: int
    origin: str
    case_title: str
    date_prom: date
    date_scraped: date
    docket: Docket | None = None
    writer: str | None = None
    category: DecisionCategory
    composition: CourtComposition
    opinions: list[ExtractOpinionPDF] = Field(default_factory=list)

    class Config:
        use_enum_values = True

    @classmethod
    def set_docket(cls, text: str):
        try:
            citation = next(extract_dockets(text))
            return Docket(
                context=text,
                short_category=citation.short_category,
                category=citation.category,
                ids=citation.ids,
                docket_date=citation.docket_date,
            )
        except Exception:
            return None

    @classmethod
    def set_writer(cls, text: str | None = None):
        if not text:
            return None
        if len(text) < 25:
            return unidecode(text).lower().strip(",.: ")

    @classmethod
    def set_opinions(cls, ops: str, id: int):
        opinions = []
        for op in json.loads(ops):
            body = op["body"]
            if op["title"] == "Notice":
                if noticed := Notice.extract(op["body"]):
                    body = noticed.txt
            opinion = ExtractOpinionPDF(
                id=op["id"],
                decision_id=id,
                pdf=op["pdf"],
                justice_label=cls.set_writer(op["writer"]),
                title=op["title"],
                body=body,
                annex=op["annex"],
            )
            opinion.set_segments()
            opinions.append(opinion)
        return opinions

    @classmethod
    def parse(
        cls,
        path: Path = PDF_DB_PATH,
        sql_file: Path = Path(__file__).parent / "sql" / "decision_list.sql",
    ) -> Iterator[Self]:
        db = Database(path)
        query = sql_file.read_text()
        rows = db.execute_returning_dicts(query)
        for row in rows:
            date_obj = parse(row["date"]).date()
            docket_str = f"{row['docket_category']} No. {row['serial']}, {date_obj.strftime('%b %-d, %Y')}"  # noqa: E501
            yield cls(
                id=row["id"],
                origin=f"{SC_BASE_URL}/{row['id']}",
                case_title=row["title"],
                date_prom=date_obj,
                date_scraped=parse(row["scraped"]).date(),
                docket=cls.set_docket(docket_str),
                composition=CourtComposition._setter(row["composition"]),
                category=DecisionCategory.set_category(
                    row.get("category", None),
                    row.get("notice", None),
                ),
                opinions=cls.set_opinions(ops=row["opinions"], id=row["id"]),
            )

    @classmethod
    def dump(cls, item: Self, target_path: Path = TARGET_FOLDER):
        if not target_path.exists():
            raise Exception("Cannot find target destination.")

        if not item.docket:
            logger.error(f"No docket in {item.id=}")
            return
        if item.docket.short_category == "BM":
            logger.error(f"Manual check: BM docket in {item.id}.")
            return

        target_id = target_path / f"{item.id}"
        target_id.mkdir(exist_ok=True)
        with open(target_id / "_pdf.yml", "w+") as f:
            logger.debug(f"Adding pdf details to {item.id=}")
            yaml.safe_dump(item.dict(), f)
