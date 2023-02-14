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

from corpus_sc_toolkit.justice import CandidateJustice, OpinionWriterName
from corpus_sc_toolkit.meta import CourtComposition, DecisionCategory

from .opinion_pdf import ExtractOpinionPDF

SC_BASE_URL = "https://sc.judiciary.gov.ph"
TARGET_FOLDER: Path = Path().home() / "code" / "corpus" / "decisions" / "sc"
SQL_DECISIONS_ONLY = Path(__file__).parent / "sql" / "limit_extract.sql"


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
    def set_opinions(cls, db: Database, ops: str, id: int, date_str: str):
        for op in json.loads(ops):
            writer, choice = None, None
            if name_obj := OpinionWriterName.extract(op["writer"]):
                writer = name_obj
                choice = CandidateJustice(db, name_obj.writer, date_str).choice
            yield ExtractOpinionPDF(
                id=op["id"],
                decision_id=id,
                pdf=f"{SC_BASE_URL}{op['pdf']}",
                writer=writer,
                justice_id=choice,
                title=op["title"],
                body=op["body"],
                annex=op["annex"],
            ).with_segments_set()

    @classmethod
    def limited_decisions(
        cls,
        db_path: Path,
        sql_query_path: Path = SQL_DECISIONS_ONLY,
    ) -> Iterator[Self]:
        db = Database(db_path)
        query = sql_query_path.read_text()
        rows = db.execute_returning_dicts(query)
        for row in rows:
            yield cls(
                id=row["id"],
                origin=f"{SC_BASE_URL}/{row['id']}",
                case_title=row["title"],
                date_prom=(date_obj := parse(row["date"]).date()),
                date_scraped=parse(row["scraped"]).date(),
                docket=cls.set_docket(
                    f"{row['docket_category']} No. {row['serial']}, {date_obj.strftime('%b %-d, %Y')}"  # noqa: E501
                ),
                category=DecisionCategory.set_category(
                    row.get("category"), row.get("notice")
                ),
                composition=CourtComposition._setter(row["composition"]),
                opinions=list(
                    cls.set_opinions(
                        db=db,
                        ops=row["opinions"],
                        id=row["id"],
                        date_str=row["date"],
                    )
                ),
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
    def export(cls, db_path: Path, to_folder: Path = TARGET_FOLDER):
        cases = cls.limited_decisions(db_path=db_path)
        for case in cases:
            case.dump(to_folder)
