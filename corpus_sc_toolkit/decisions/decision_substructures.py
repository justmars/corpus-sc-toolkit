import re
from collections.abc import Iterator

from citation_utils import Citation
from pydantic import BaseModel, Field
from statute_patterns import count_rules

from ..utils import download_to_temp, segmentize
from ._resources import BUCKET_NAME, CLIENT, ORIGIN

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
                if tx := download_to_temp(
                    bucket=ORIGIN, src=content["Key"], ext="md"
                ):
                    if isinstance(tx, str):
                        yield cls(
                            id=f"{decision_id}-{key}",
                            decision_id=decision_id,
                            title=DecisionOpinion.get_headline(tx),
                            text=tx,
                            justice_id=justice_id,
                        )
