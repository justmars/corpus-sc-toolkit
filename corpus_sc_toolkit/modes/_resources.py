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
from .txt.splitter import segmentize

"""Generic temporary file download."""

TEMP_FOLDER = Path(__file__).parent.parent / "tmp"
TEMP_FOLDER.mkdir(exist_ok=True)


def tmp_load(src: str, ext: str = "yaml") -> str | dict[str, Any] | None:
    """Based on the `src` prefix, download the same into a temp file
    and return its contents based on the extension. A `yaml` extension
    should result in contents in `dict` format; where an `md` or `html`
    extension results in `str`. The temp file is deleted after every
    successful extraction of the `src` as content."""

    path = TEMP_FOLDER / f"temp.{ext}"
    ORIGIN.download(src, str(path))
    content = None
    if ext == "yaml":
        content = yaml.safe_load(path.read_bytes())
    elif ext in ["md", "html"]:
        content = path.read_text()
    path.unlink(missing_ok=True)
    return content


"""Decision substructures: opinions and segments."""


class OpinionSegment(NamedTuple):
    id: str
    opinion_id: str
    decision_id: str
    position: str
    segment: str
    char_count: int


OPINION_MD_H1 = re.compile(r"^#\s*(?P<label>).*$")


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
    def segments(self) -> Iterator[OpinionSegment]:
        """Auto-generated segments based on the text of the opinion."""
        for extract in segmentize(self.text):
            yield OpinionSegment(
                id=f"{self.id}-{extract['position']}",
                decision_id=self.decision_id,
                opinion_id=self.id,
                **extract,
            )

    @classmethod
    def get_headline(cls, text: str) -> str:
        if match := OPINION_MD_H1.search(text):
            return match.group("label")
        return "Not Found"

    @classmethod
    def key_from_md_prefix(cls, prefix: str):
        """Given a prefix containing a filename, e.g. `/hello/test/ponencia.md`,
        get the identifying key of the filename, e.g. `ponencia`."""
        if "/" in prefix and prefix.endswith(".md"):
            return prefix.split("/")[-1].split(".")[0]
        return "Invalid Key."

    @classmethod
    def fetch(
        cls,
        opinion_prefix: str,
        decision_id: str,
        ponente_id: int | None = None,
    ):
        """The `opinion_prefix` must be in the form of:

        `<docket>/<year>/<month>/<serial>/opinions/`. Note the ending backslash.

        The `ponente_id`, if present, will be used to populate the ponencia
        opinion."""
        result = CLIENT.list_objects_v2(
            Bucket=BUCKET_NAME, Delimiter="/", Prefix=opinion_prefix
        )
        for content in result["Contents"]:
            if content["Key"].endswith(".md"):
                key = DecisionOpinion.key_from_md_prefix(content["Key"])
                justice_id = ponente_id if key == "ponencia" else int(key)
                if text := tmp_load(content["Key"], ext="md"):
                    if isinstance(text, str):
                        yield cls(
                            id=f"{decision_id}-{key}",
                            decision_id=decision_id,
                            title=DecisionOpinion.get_headline(text),
                            text=text,
                            tags=[],
                            justice_id=justice_id,
                        )


"""Decision structure aspects."""

DOCKETS: list[str] = ["GR", "AM", "OCA", "AC", "BM"]
"""Default selection of docket types to serve as root prefixes in R2."""

SC_START_YEAR = 1902
PRESENT_YEAR = datetime.datetime.now().date().year
YEARS: tuple[int, int] = (SC_START_YEAR, PRESENT_YEAR)
"""Default range of years to serve as prefixes in R2"""

PDF_DECISION_SQL = Path(__file__).parent / "sql" / "limit_extract.sql"
SQL_QUERY = PDF_DECISION_SQL.read_text()
"""Queries PDF-based tables for the a list of decisions and opinions."""

BUCKET_NAME = "sc-decisions"
ORIGIN = CFR2_Bucket(name=BUCKET_NAME)
meta = ORIGIN.resource.meta
if not meta:
    raise Exception("Bad bucket.")
CLIENT = meta.client
"""R2 variables in order to perform operations from the library."""


class DecisionFields(BaseModel):
    """
    # Decision Fields

    A `Decision` relies on pre-processing of various fields.

    This toolkit helps process some of those fields prior to insertion into a
    terminal database (even if they may previously originate from another
    third-party database.)

    Field | Type | Description
    :--:|:--:|:--
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
    opinions: list[DecisionOpinion] = Field(default_factory=list)

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
        """When uploading to R2, the metadata can be included as extra arguments to
        the file."""
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
    def get_dated_prefixes(
        cls, dockets: list[str] = DOCKETS, years: tuple[int, int] = YEARS
    ) -> Iterator[str]:
        """Results in the following prefix format: `<docket>/<year>/<month>`
        in ascending order."""
        for docket in dockets:
            cnt_year, end_year = years[0], years[1]
            while cnt_year <= end_year:
                for month in range(1, 13):
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
                Bucket=BUCKET_NAME, Delimiter="/", Prefix=prefix
            )
