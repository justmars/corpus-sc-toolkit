import json
from collections.abc import Iterator
from datetime import date

from pathlib import Path
from typing import Self

import yaml
from citation_docket import Docket, extract_dockets
from dateutil.parser import parse
from loguru import logger
from pydantic import BaseModel, Field
from sqlite_utils import Database
from corpus_toolkit.components import DecisionCategory, CourtComposition
from corpus_toolkit.justice import OpinionWriterName

SC_BASE_URL = "https://sc.judiciary.gov.ph"
PDF_DB_PATH: Path = Path().cwd() / "pdf.db"
TARGET_FOLDER: Path = Path().home() / "code" / "corpus" / "decisions" / "sc"

SQL_DECISIONS_ONLY = Path(__file__).parent / "sql" / "limit_extract.sql"


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
    writer: OpinionWriterName | None = Field(
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

    def get_segment(
        self, elements: list, opinion_id: str, text: str, position: str
    ):
        if all(elements):
            return ExtractSegmentPDF(
                id="-".join(str(i) for i in elements),
                opinion_id=opinion_id,
                decision_id=self.decision_id,
                position=position,
                segment=text,
                char_count=len(text),
            )

    def _from_main(self, db: Database) -> Self:
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
                self.segments.append(segment)
        return self

    def _from_opinions(self, db: Database) -> Self:
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
                self.segments.append(segment)
        return self

    def with_segments_set(self, path: Path = PDF_DB_PATH) -> Self:
        db = Database(path)
        # the title is set in limit_extract.sql
        is_main = self.title in ["Ponencia", "Notice"]
        return self._from_main(db) if is_main else self._from_opinions(db)


class ExtractDecisionPDF(BaseModel):
    id: int
    origin: str
    case_title: str
    date_prom: date
    date_scraped: date
    docket: Docket | None = None
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
    def set_opinions(cls, ops: str, id: int):
        for op in json.loads(ops):
            yield ExtractOpinionPDF(
                id=op["id"],
                decision_id=id,
                pdf=f"{SC_BASE_URL}{op['pdf']}",
                writer=OpinionWriterName.extract(op["writer"]),
                title=op["title"],
                body=op["body"],
                annex=op["annex"],
            ).with_segments_set()

    @classmethod
    def limited_decisions(
        cls,
        db_path: Path = PDF_DB_PATH,
        sql_query_path: Path = SQL_DECISIONS_ONLY,
    ) -> Iterator[Self]:
        db = Database(db_path)
        query = sql_query_path.read_text()
        rows = db.execute_returning_dicts(query)
        for row in rows:
            scrape_date = parse(row["scraped"]).date()
            date_obj = parse(row["date"]).date()
            docket_str = f"{row['docket_category']} No. {row['serial']}, {date_obj.strftime('%b %-d, %Y')}"  # noqa: E501
            docket = cls.set_docket(docket_str)
            category = DecisionCategory.set_category(
                row.get("category"), row.get("notice")
            )
            composition = CourtComposition._setter(row["composition"])
            op_list = list(cls.set_opinions(ops=row["opinions"], id=row["id"]))
            yield cls(
                id=row["id"],
                origin=f"{SC_BASE_URL}/{row['id']}",
                case_title=row["title"],
                date_prom=date_obj,
                date_scraped=scrape_date,
                docket=docket,
                category=category,
                composition=composition,
                opinions=op_list,
            )

    @property
    def is_dump_ok(self, target_path: Path = TARGET_FOLDER):
        if not target_path.exists():
            raise Exception("Cannot find target destination.")
        if not self.docket:
            logger.warning(f"No docket in {self.id=}")
            return False
        if self.docket.short_category == "BM":
            logger.warning(f"Manual check: BM docket in {self.id}.")
            return False
        return True

    def dump(self, target_path: Path = TARGET_FOLDER):
        if not self.is_dump_ok:
            return
        target_id = target_path / f"{self.id}"
        target_id.mkdir(exist_ok=True)
        with open(target_id / "_pdf.yml", "w+") as writefile:
            yaml.safe_dump(self.dict(), writefile)
            logger.debug(f"Built {target_id=}=")

    @classmethod
    def export(
        cls,
        from_db_path: Path = PDF_DB_PATH,
        to_folder: Path = TARGET_FOLDER,
    ):
        for case in cls.limited_decisions(
            db_path=from_db_path,
            sql_query_path=SQL_DECISIONS_ONLY,
        ):
            case.dump(to_folder)
