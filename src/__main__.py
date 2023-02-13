from pathlib import Path
import re
import json
from datetime import date
from enum import Enum
from sqlite_utils import Database
from pydantic import BaseModel, Field
from citation_docket import extract_dockets, Docket
from dateutil.parser import parse
from unidecode import unidecode

target_folder: Path = Path().home() / "code" / "corpus" / "decisions"
decision_list_sql: Path = Path(__file__).parent / "sql" / "decision_list.sql"

CATEGORY_START_DECISION = re.compile(r"d\s*e\s*c", re.I)
CATEGORY_START_RESOLUTION = re.compile(r"r\s*e\s*s", re.I)

COMPOSITION_START_DIVISION = re.compile(r"div", re.I)
COMPOSITION_START_ENBANC = re.compile(r"en", re.I)


def init_surnames(text: str):
    """Remove unnecessary text and make uniform accented content."""
    text = unidecode(text)
    text = text.lower()
    text = text.strip(",.: ")
    return text


class DecisionSource(str, Enum):
    sc = "sc"
    legacy = "legacy"


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


class ExtractDecisionPDF(BaseModel):
    id: int
    title: str
    date: date
    docket: Docket | None = None
    writer: str | None = None
    category: DecisionCategory
    composition: CourtComposition
    opinions: list[ExtractOpinionPDF] = Field(default_factory=list)

    class Config:
        use_enum_values = True

    @classmethod
    def get_docket(cls, text: str):
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
    def get_writer(cls, text: str | None = None):
        if not text:
            return None
        if len(text) < 25:
            return init_surnames(text)

    @classmethod
    def parse(cls, path: Path = Path().cwd() / "pdf.db"):
        db = Database(path)
        for row in db.execute_returning_dicts(decision_list_sql.read_text()):
            date_obj = parse(row["date"]).date()
            docket = cls.get_docket(
                f"{row['docket_category']} No. {row['serial']}, {date_obj.strftime('%b %-d, %Y')}"
            )
            yield cls(
                id=row["id"],
                title=row["title"],
                date=date_obj,
                composition=CourtComposition._setter(row["composition"]),
                category=DecisionCategory.set_category(
                    row.get("category", None), row.get("notice", None)
                ),
                docket=docket,
                opinions=[
                    ExtractOpinionPDF(
                        id=op["id"],
                        decision_id=row["id"],
                        pdf=op["pdf"],
                        justice_label=cls.get_writer(op["writer"]),
                        title=op["title"],
                        body=op["body"],
                        annex=op["annex"],
                    )
                    for op in json.loads(row["opinions"])
                ],
            )
