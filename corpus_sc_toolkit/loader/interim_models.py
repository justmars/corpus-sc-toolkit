import yaml
from citation_utils import Citation, ShortDocketCategory
from loguru import logger
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any, Self
import json
from pydantic import BaseModel, Field
from sqlite_utils import Database

from corpus_sc_toolkit.meta import (
    CourtComposition,
    DecisionCategory,
    DecisionSource,
)
from corpus_sc_toolkit.resources import SC_LOCAL_FOLDER, SC_BASE_URL
from corpus_sc_toolkit.justice import CandidateJustice


class InterimSegment(BaseModel):
    id: str = Field(...)
    opinion_id: str = Field(...)
    decision_id: int = Field(...)
    position: str = Field(...)
    segment: str = Field(...)
    char_count: int = Field(...)


class InterimOpinion(BaseModel):
    id: str = Field(...)
    decision_id: int = Field(...)
    pdf: str
    candidate: CandidateJustice
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
    segments: list[InterimSegment] = Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def setup(cls, db: Database, data: dict) -> dict | None:
        """Presumes existence of the following keys:

        This will partially process the sql query defined in
        `/sql/limit_extract.sql`

        The required fields in `data`:

        1. `opinions` - i.e. a string made of `json_group_array`, `json_object` from sqlite query
        2. `id` - the decision id connected to each opinion from the opinions list
        3. `date` - for determining the justice involved in the opinion/s
        """  # noqa: E501
        match = None
        opinions = []
        keys = ["opinions", "id", "date"]
        if not all([data.get(k) for k in keys]):
            return None

        id, dt, op_lst = data["id"], data["date"], json.loads(data["opinions"])

        for op in op_lst:
            pdf_url = f"{SC_BASE_URL}{op['pdf']}"
            candidate = CandidateJustice(db, op.get("writer"), dt)
            obj = cls(
                id=op["id"],
                decision_id=id,
                pdf=pdf_url,
                candidate=candidate,
                title=op["title"],
                body=op["body"],
                annex=op["annex"],
            )
            opinion = obj.with_segments_set(db=db)
            opinions.append(opinion)

            if not match and opinion.title == "Ponencia":
                match = opinion.candidate

        details = match.detail._asdict() if match and match.detail else {}
        return {"opinions": opinions} | details

    def get_segment(
        self,
        elements: list,
        opinion_id: str,
        text: str,
        position: str,
    ):
        if all(elements):
            return InterimSegment(
                id="-".join(str(i) for i in elements),
                opinion_id=opinion_id,
                decision_id=self.decision_id,
                position=position,
                segment=text,
                char_count=len(text),
            )

    def _from_main(self, db: Database) -> Iterator[InterimSegment]:
        """Populate segments from the main decision."""
        criteria = "decision_id = ? and length(text) > 10"
        params = (self.decision_id,)
        rows = db["pre_tbl_decision_segment"].rows_where(criteria, params)
        for row in rows:
            if segment := self.get_segment(
                elements=[row["id"], row["page_num"], self.decision_id],
                opinion_id=f"main-{self.decision_id}",
                text=row["text"],
                position=f"{row['id']}-{row['page_num']}",
            ):
                yield segment

    def _from_opinions(self, db: Database) -> Iterator[InterimSegment]:
        """Populate segments from the opinion decision."""
        criteria = "opinion_id = ? and length(text) > 10"
        params = (self.id,)
        rows = db["pre_tbl_opinion_segment"].rows_where(criteria, params)
        for row in rows:
            if segment := self.get_segment(
                elements=[row["id"], row["page_num"], row["opinion_id"]],
                opinion_id=f"{str(self.decision_id)}-{row['opinion_id']}",
                text=row["text"],
                position=f"{row['id']}-{row['page_num']}",
            ):
                yield segment

    def with_segments_set(self, db: Database) -> Self:
        if self.title in ["Ponencia", "Notice"]:  # see limit_extract.sql
            self.segments = list(self._from_main(db))
        else:
            self.segments = list(self._from_opinions(db))
        return self


class InterimDecision(BaseModel):
    id: str
    source: DecisionSource = DecisionSource.sc
    origin: str
    case_title: str
    date_prom: date
    date_scraped: date
    citation: Citation | None = None
    composition: CourtComposition
    category: DecisionCategory
    raw_ponente: str | None = None
    justice_id: str | None = None
    per_curiam: bool = False
    opinions: list[InterimOpinion] = Field(default_factory=list)

    class Config:
        use_enum_values = True

    @classmethod
    def limited_decisions(cls, db: Database) -> Iterator[Self]:
        from .from_pdf import decision_from_pdf_db

        sql_path = Path(__file__).parent / "sql" / "limit_extract.sql"
        query = sql_path.read_text()
        rows = db.execute_returning_dicts(query)
        for row in rows:
            if result := decision_from_pdf_db(db, row):
                yield result

    @property
    def is_dump_ok(self, target_path: Path = SC_LOCAL_FOLDER):
        if not target_path.exists():
            raise Exception("Cannot find target destination.")
        if not self.citation:
            logger.warning(f"No docket in {self.id=}")
            return False
        if self.citation.docket_category == ShortDocketCategory.BM:
            logger.warning(f"Manual check: BM docket in {self.id}.")
            return False
        return True

    def dump(self, target_path: Path = SC_LOCAL_FOLDER):
        if not self.is_dump_ok:
            return
        target_id = target_path / f"{self.id}"
        target_id.mkdir(exist_ok=True)
        with open(target_id / "_pdf.yml", "w+") as writefile:
            yaml.safe_dump(self.dict(), writefile)
            logger.debug(f"Built {target_id=}=")

    @classmethod
    def export(cls, db: Database, to_folder: Path = SC_LOCAL_FOLDER):
        for case in cls.limited_decisions(db):
            case.dump(to_folder)
