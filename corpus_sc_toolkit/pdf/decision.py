from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Self, Any

import yaml
from citation_utils import Citation, ShortDocketCategory
from dateutil.parser import parse
from loguru import logger
from pydantic import BaseModel, Field
from sqlite_utils import Database

from corpus_sc_toolkit.meta import (
    CourtComposition,
    DecisionCategory,
    DecisionSource,
    get_cite_from_fields,
    get_id_from_citation,
)
from .resources import SC_BASE_URL, TARGET_FOLDER
from .opinion import InterimOpinion


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
    def set(cls, row: dict[str, Any], opx: dict[str, Any], cite: Citation):
        id = get_id_from_citation(
            folder_name=row["id"],
            source=DecisionSource.sc.value,
            citation=cite,
        )
        cat = DecisionCategory.set_category(
            category=row.get("category"),
            notice=row.get("notice"),
        )
        return cls(
            id=id,
            origin=f"{SC_BASE_URL}/{row['id']}",
            case_title=row["title"],
            date_prom=parse(row["date"]).date(),
            date_scraped=parse(row["scraped"]).date(),
            citation=cite,
            composition=CourtComposition._setter(text=row["composition"]),
            category=cat,
            opinions=opx["opinions"],
            raw_ponente=opx.get("raw_ponente", None),
            per_curiam=opx.get("per_curiam", False),
            justice_id=opx.get("justice_id", None),
        )

    @classmethod
    def limited_decisions(cls, db: Database) -> Iterator[Self]:
        sql_path = Path(__file__).parent / "sql" / "limit_extract.sql"
        query = sql_path.read_text()
        rows = db.execute_returning_dicts(query)
        for row in rows:
            if not (cite := get_cite_from_fields(row)):
                logger.error(f"Bad citation in {row['id']=}")
                continue
            opx = InterimOpinion.setup(db, row)
            if not opx or not opx.get("opinions"):
                logger.error(f"No opinions detected in {row['id']=}")
                continue
            yield cls.set(row, opx, cite)

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
    def export(cls, db: Database, to_folder: Path = TARGET_FOLDER):
        for case in cls.limited_decisions(db):
            case.dump(to_folder)
