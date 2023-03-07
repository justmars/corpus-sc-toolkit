import re
from collections.abc import Iterator
from pathlib import Path

from citation_utils import Citation
from pydantic import BaseModel, Field
from statute_patterns import count_rules

from .._utils import segmentize
from ._resources import DECISION_BUCKET_NAME, DECISION_CLIENT, decision_storage

"""Decision substructures: opinions and segments."""


class OpinionSegment(BaseModel):
    """A decision is naturally subdivided into [opinions][decision opinions].
    Breaking down opinions into segments is an attempt to narrow down the scope
    of decisions to smaller portions for purposes of FTS search snippets and analysis.
    """

    opinion_id: str  # overriden in decisions.py
    decision_id: str  # overriden in decisions.py
    id: str = Field(..., col=str)
    position: str = Field(
        default=...,
        title="Relative Position",
        description="Line number of text stripped from source.",
        col=int,
        index=True,
    )
    char_count: int = Field(
        default=...,
        title="Character Count",
        description="Makes it easier to discover patterns.",
        col=int,
        index=True,
    )
    segment: str = Field(
        default=...,
        title="Body Segment",
        description="Partial fragment of opinion.",
        col=str,
        fts=True,
    )


OPINION_MD_H1 = re.compile(r"^#\s*(?P<label>).*$")


class DecisionOpinion(BaseModel):
    """A decision may contain a single opinion entitled the Ponencia or span
    multiple opinions depending on the justices of the Court who are charged to decide
    a specific case.
    """

    decision_id: str  # overriden in decisions.py
    justice_id: int | None = None
    id: str = Field(
        ...,
        title="Opinion ID",
        description=(
            "Based on combining decision_id with the justice_id, if found."
        ),
        col=str,
    )
    pdf: str | None = Field(
        default=None,
        title="PDF URL",
        description="Links to downloadable PDF, if it exists",
        col=str,
    )
    title: str | None = Field(
        ...,
        description="How opinion called, e.g. Ponencia, Concurring Opinion,",
        col=str,
    )
    tags: list[str] | None = Field(
        default=None,
        description="e.g. main, dissenting, concurring, separate",
    )
    remark: str | None = Field(
        default=None,
        title="Short Remark on Opinion",
        description="e.g. 'I reserve my right, etc.', 'On leave.', etc.",
        col=str,
        fts=True,
    )
    concurs: list[dict] | None = Field(default=None)
    text: str = Field(
        ...,
        description="Text proper of opinion (ideally in markdown)",
        col=str,
        fts=True,
    )

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

    @property
    def rules(self) -> Iterator[dict]:
        """Get the statutes found in the text."""
        return count_rules(self.text)

    @property
    def citations(self) -> Iterator[Citation]:
        """Get the citations found in the text."""
        return Citation.extract_citations(self.text)

    @classmethod
    def get_headline(cls, text: str) -> str:
        if match := OPINION_MD_H1.search(text):
            return match.group("label")
        return "Not Found"

    @classmethod
    def key_from_md_prefix(cls, prefix: str) -> str | None:
        """Given a prefix containing a filename, e.g. `/hello/test/ponencia.md`,
        get the identifying key of the filename, e.g. `ponencia`."""
        if "/" in prefix and prefix.endswith(".md"):
            return prefix.split("/")[-1].split(".")[0]
        return None

    @classmethod
    def make(
        cls,
        origin_path_str: str,
        decision_id: str,
        justice_id: int | None,
        text: str,
    ):
        """Common opinion instantiator for both `cls.from_folder()` and
        `cls.from_storage()`"""
        if key := cls.key_from_md_prefix(origin_path_str):
            justice_id = justice_id if key == "ponencia" else int(key)
            return cls(
                id=f"{decision_id}-{key}",
                decision_id=decision_id,
                title=cls.get_headline(text),
                text=text,
                justice_id=justice_id,
            )
        return None

    @classmethod
    def from_folder(
        cls,
        opinions_folder: Path,
        decision_id: str,
        ponente_id: int | None = None,
    ):
        """Assumes a local folder containing opinions in .md format.
        The `ponente_id`, if present, will be used to populate the ponencia
        opinion."""
        for opinion_path in opinions_folder.glob("*.md"):
            if opinion := cls.make(
                origin_path_str=str(opinion_path),
                decision_id=decision_id,
                justice_id=ponente_id,
                text=opinion_path.read_text(),
            ):
                yield opinion

    @classmethod
    def from_storage(
        cls,
        opinion_prefix: str,
        decision_id: str,
        ponente_id: int | None = None,
    ):
        """`opinion_prefix` format: `<docket>/<year>/<month>/<serial>/opinions/`.
        Note ending backslash.

        The `ponente_id`, if present, will be used to populate the ponencia
        opinion."""
        result = DECISION_CLIENT.list_objects_v2(
            Bucket=DECISION_BUCKET_NAME, Delimiter="/", Prefix=opinion_prefix
        )
        for content in result["Contents"]:
            if content["Key"].endswith(".md"):
                if text := decision_storage.restore_temp_txt(content["Key"]):
                    if opinion := cls.make(
                        origin_path_str=str(content["Key"]),
                        decision_id=decision_id,
                        justice_id=ponente_id,
                        text=text,
                    ):
                        yield opinion
