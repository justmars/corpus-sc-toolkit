import re
import datetime
import yaml
from typing import NamedTuple, Any
from pathlib import Path
from start_sdk import CFR2_Bucket
from citation_utils import Citation
from collections.abc import Iterator
from pydantic import BaseModel, Field

from corpus_sc_toolkit.meta import (
    CourtComposition,
    DecisionCategory,
    DecisionSource,
)

bucket_name = "sc-decisions"
origin = CFR2_Bucket(name=bucket_name)
meta = origin.resource.meta
if not meta:
    raise Exception("Bad bucket.")
CLIENT = meta.client

TEMP_FOLDER = Path(__file__).parent.parent / "tmp"
TEMP_FOLDER.mkdir(exist_ok=True)

DOCKETS: list[str] = ["GR", "AM", "OCA", "AC", "BM"]

SC_START_YEAR = 1902
PRESENT_YEAR = datetime.datetime.now().date().year
YEARS: tuple[int, int] = (SC_START_YEAR, PRESENT_YEAR)
months = range(1, 13)

PDF_DECISION_SQL = Path(__file__).parent / "sql" / "limit_extract.sql"
SQL_QUERY = PDF_DECISION_SQL.read_text()

OPINION_MD_H1 = re.compile(r"^#\s*(?P<label>).*$")


def get_headline(text: str) -> str:
    if m := OPINION_MD_H1.search(text):
        return m.group("label")
    return "Not Found"


class DecisionFields(BaseModel):
    """
    # Decision Fields

    A `Decision` relies on pre-processing of various fields.

    This toolkit helps process some of those fields prior to insertion into a
    terminal database (even if they may previously originate from another
    third-party database.)

    Field | Type | Description
    :--:|:--:|:--
    created | float | When was this model instantiated, for paths, this is when the file was actually made
    modified | float |  When was this model last modified, for paths, this is when the file was actually modified
    id | str | The [combination of various strings][set-decision-id-from-values] based on the source and citation, if available.
    origin | str | Where the decision was sourced from
    title | str | The case title, this can be classified into [tags][title-tags]
    description | str | The citation display
    date | datetime.date | The date the case was promulgated
    date_scraped | datetime.date | The date the case was scraped
    citation | optional[Citation] | The citation object
    composition | [CourtComposition][court-composition] | Whether the court sat en banc or in division
    category | [DecisionCategory][decision-category] | Whether the case decided was a decision or a resolution
    raw_ponente| optional[str] | Who decided the case, if available
    justice_id | optional[int] | The [justice id][justice], if available
    per_curiam | bool. Defaults to False. | Whether the case was decided per curiam
    is_pdf | bool. Defaults to False. | Whether the case originated from a PDF file
    fallo | optional[str] | Detected fallo / dispositive portion
    voting | optional[str] | Detected [voting line][vote-lines]
    emails | list[str] | Emails of authors
    """  # noqa: E501

    created: float
    modified: float
    origin: str
    title: str
    description: str
    date: datetime.date
    date_scraped: datetime.date
    citation: Citation | None = None
    composition: CourtComposition
    category: DecisionCategory
    raw_ponente: str | None = None
    justice_id: int | None = None
    per_curiam: bool = False
    is_pdf: bool = False
    fallo: str | None = None
    voting: str | None = None
    emails: list[str] = Field(default_factory=list)

    class Config:
        use_enum_values = True

    @property
    def source(self) -> str:
        """See [DecisionSource][decision-source], may either be `sc` or `legacy`,
        depending on the `date` of the instance."""
        return DecisionSource.from_date(self.date)

    @property
    def docket_citation(self) -> Citation | None:
        """Check if a valid docket citation exists and return the same."""
        if not self.citation:
            return None
        if not self.citation.docket_serial:
            return None
        if not self.citation.docket_category:
            return None
        return self.citation

    @property
    def id(self) -> str | None:
        """Generate an id based on a prefix base, e.g. if the `@base_prefix` is
        `GR/2021/10/227403`, the generated id will be gr-2021-10-227403."""
        if not self.base_prefix:
            return None
        return self.base_prefix.replace("/", "-").lower()

    @property
    def base_prefix(self) -> str | None:
        """If the model were to be stored in cloud storage like R2,
        this property ensures a unique prefix for the instance. Should
        be in the following format: `<category>/<year>/<month>/<serial>`,
        e.g. `GR/2021/10/227403`
        """
        if not self.docket_citation:
            return None
        return "/".join(
            str(i)
            for i in [
                self.docket_citation.docket_category,
                self.date.year,
                self.date.month,
                self.docket_citation.docket_serial,
            ]
        )

    @property
    def meta(self):
        if not self.docket_citation:
            return {}
        raw = {
            "Decision_Title": self.title,
            "Decision_Category": self.category,
            "Court_Composition": self.composition,
            "Docket_Category": self.docket_citation.docket_category,
            "Docket_ID": self.docket_citation.docket_serial,
            "Docket_Date": self.date.isoformat(),
            "Report_Phil": self.docket_citation.phil,
            "Report_Scra": self.docket_citation.scra,
            "Report_Off_Gaz": self.docket_citation.offg,
        }
        return {"Metadata": {k: str(v) for k, v in raw.items() if v}}

    @classmethod
    def set_id(cls, prefix: str):
        """Converts a prefix to a slug."""
        return prefix.removesuffix("/").replace("/", "-").lower()

    @classmethod
    def key_from_md_prefix(cls, prefix: str):
        """Given a prefix containing a filename, e.g. `/hello/test/ponencia.md`,
        get the identifying key of the filename, e.g. `ponencia`."""
        if "/" in prefix and prefix.endswith(".md"):
            return prefix.split("/")[-1].split(".")[0]
        return "Invalid Key."

    @classmethod
    def get_dated_prefixes(
        cls, dockets: list[str] = DOCKETS, years: tuple[int, int] = YEARS
    ) -> Iterator[str]:
        """Results in the following prefix format: `<docket>/<year>/<month>`
        in ascending order."""
        for docket in dockets:
            cnt_year, end_year = years[0], years[1]
            while cnt_year <= end_year:
                for month in months:
                    yield f"{docket}/{cnt_year}/{month}/"
                cnt_year += 1

    @classmethod
    def iter_collections(
        cls, dockets: list[str] = DOCKETS, years: tuple[int, int] = YEARS
    ) -> Iterator[dict[str, Any]]:
        """Based on a list of prefixes ordered by date, get the list of objects
        per prefix. Each item in the collection is a dict which will contain
        a `CommonPrefixes` key."""
        for prefix in cls.get_dated_prefixes(dockets, years):
            yield CLIENT.list_objects_v2(
                Bucket=bucket_name, Delimiter="/", Prefix=prefix
            )

    @classmethod
    def tmp_load(
        cls, src: str, ext: str = "yaml"
    ) -> str | dict[str, Any] | None:
        """Based on the `src` prefix, download the same into a temp file
        and return its contents based on the extension. A `yaml` extension
        should result in contents in `dict` format; where an `md` or `html`
        extension results in `str`. The temp file is deleted after every
        successful extraction of the `src` as content."""

        path = TEMP_FOLDER / f"temp.{ext}"
        origin.download(src, str(path))
        content = None
        if ext == "yaml":
            content = yaml.safe_load(path.read_bytes())
        elif ext in ["md", "html"]:
            content = path.read_text()
        path.unlink(missing_ok=True)
        return content


class OpinionSegment(NamedTuple):
    id: str
    opinion_id: str
    decision_id: str
    position: str
    segment: str
    char_count: int

    @classmethod
    def set_from_txt(
        cls, decision_id: str, opinion_id: str, opinion_text: str
    ):
        from corpus_sc_toolkit import segmentize

        for extract in segmentize(opinion_text):
            yield cls(
                id=f"{opinion_id}-{extract['position']}",
                decision_id=decision_id,
                opinion_id=opinion_id,
                **extract,
            )


class DecisionOpinion(NamedTuple):
    id: str
    decision_id: str
    title: str
    text: str
    tags: list[str]
    pdf: str | None = None
    remark: str | None = None
    concurs: list[dict] | None = None
    justice_id: int | None = None

    @property
    def segments(self) -> list[OpinionSegment]:
        return list(
            OpinionSegment.set_from_txt(
                decision_id=self.decision_id,
                opinion_id=self.id,
                opinion_text=self.text,
            )
        )
