import json
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Self

import yaml
from citation_utils import Citation, ShortDocketCategory
from dateutil.parser import parse
from loguru import logger
from pydantic import BaseModel, Field
from sqlite_utils import Database

from corpus_sc_toolkit.citation import get_id_from_citation as get_id
from corpus_sc_toolkit.justice import CandidateJustice, OpinionWriterName
from corpus_sc_toolkit.meta import (
    CourtComposition,
    DecisionCategory,
    DecisionSource,
)

from .opinion_pdf import ExtractOpinionPDF

SC_BASE_URL = "https://sc.judiciary.gov.ph"
TARGET_FOLDER: Path = Path().home() / "code" / "corpus" / "decisions" / "sc"
SQL_DECISIONS_ONLY = Path(__file__).parent / "sql" / "limit_extract.sql"


class PonenciaMeta(BaseModel):
    raw_ponente: str | None = Field(
        None,
        title="Ponente",
        description=(
            "After going through a cleaning process, this should be in"
            " lowercase and be suitable for matching a justice id."
        ),
    )
    justice_id: int | None = Field(
        None,
        title="Justice ID",
        description=(
            "Using the raw_ponente, determine the appropriate justice_id using"
            " the `update_justice_ids.sql` template."
        ),
    )
    per_curiam: bool = Field(
        False,
        title="Is Per Curiam",
        description="If true, decision was penned anonymously.",
    )


class ExtractDecisionPDF(BaseModel):
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
    opinions: list[ExtractOpinionPDF] = Field(default_factory=list)

    class Config:
        use_enum_values = True

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
            id = row["id"]
            dt = row["date"]
            src = DecisionSource.sc.value
            composition = CourtComposition._setter(text=row["composition"])
            category = DecisionCategory.set_category(
                category=row.get("category"),
                notice=row.get("notice"),
            )

            date_obj = parse(dt).date()
            docket_partial = f"{row['docket_category']} No. {row['serial']}"
            docket_str = f"{docket_partial}, {date_obj.strftime('%b %-d, %Y')}"
            cite = Citation.extract_citation(docket_str)
            if not cite:
                logger.error(f"Bad citation in {id=}")
                continue

            ops = row["opinions"]
            op_lst = list(cls.set_opinions(db=db, ops=ops, id=id, date_str=dt))
            if not op_lst:
                logger.error(f"No opinions detected in {id=}")
                continue

            ponencias = [o for o in op_lst if o.title == "Ponencia"]
            if not ponencias:
                logger.error(f"Could not detect ponencia in {id=}")
                continue
            p = ponencias[0]
            if p.writer:
                per_curiam = p.writer.per_curiam
                raw_ponente = p.writer.writer
                justice_id = p.justice_id["id"] if p.justice_id else None
            else:
                per_curiam = False
                raw_ponente = None
                justice_id = None

            yield cls(
                id=get_id(folder_name=id, source=src, citation=cite),
                origin=f"{SC_BASE_URL}/{id}",
                case_title=row["title"],
                date_prom=date_obj,
                date_scraped=parse(row["scraped"]).date(),
                citation=cite,
                composition=composition,
                category=category,
                opinions=op_lst,
                raw_ponente=raw_ponente,
                per_curiam=per_curiam,
                justice_id=justice_id,
            )

    @property
    def is_dump_ok(self, target_path: Path = TARGET_FOLDER):
        if not target_path.exists():
            raise Exception("Cannot find target destination.")
        if not self.citation:
            logger.warning(f"No docket in {self.id=}")
            return False
        if self.citation.docket_category == ShortDocketCategory.BM:
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
