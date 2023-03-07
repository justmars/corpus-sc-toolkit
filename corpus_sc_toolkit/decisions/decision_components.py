import re
from collections.abc import Iterator
from pathlib import Path
from typing import Self

from citation_utils import Citation
from loguru import logger
from pydantic import BaseModel, Field
from statute_patterns import count_rules
from statute_trees import StatuteBase

from .._utils import segmentize
from ._resources import DECISION_BUCKET_NAME, DECISION_CLIENT, decision_storage


class MentionedStatute(StatuteBase):
    mentions: int

    @classmethod
    def set_counted_statute(cls, text: str):
        for rule in count_rules(text):
            if mentions := rule.get("mentions"):
                if isinstance(mentions, int) and mentions >= 1:
                    yield cls(
                        statute_category=rule.get("cat"),
                        statute_serial_id=rule.get("id"),
                        mentions=mentions,
                    )


class OpinionSegment(BaseModel):
    """A decision is naturally subdivided into [opinions][decision opinions].
    Breaking down opinions into segments is an attempt to narrow down the scope
    of decisions to smaller portions for purposes of FTS search snippets and analysis.
    """

    id: str = Field(..., col=str)
    opinion_id: str  # later replaced in decisions.py
    decision_id: str  # later replaced in decisions.py
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

    @classmethod
    def make_segments(
        cls, decision_id: str, opinion_id: str, text: str
    ) -> Iterator[Self]:
        """Auto-generated segments based on the text of the opinion."""
        for extract in segmentize(text):
            yield cls(
                id=f"{opinion_id}-{extract['position']}",
                decision_id=decision_id,
                opinion_id=opinion_id,
                **extract,
            )


OPINION_MD_H1 = re.compile(r"^#\s*(?P<label>).*$")


class DecisionOpinion(BaseModel):
    """A decision may contain a single opinion entitled the Ponencia or span
    multiple opinions depending on the justices of the Court who are charged to decide
    a specific case.
    """

    id: str = Field(..., title="Opinion ID", col=str)
    decision_id: str  # later replaced in decision.py
    justice_id: int | None = None
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
    statutes: list[MentionedStatute]
    segments: list[OpinionSegment]
    citations: list[Citation]

    @classmethod
    def get_headline(cls, text: str) -> str | None:
        """Markdown contains H1 header, extract this header."""
        if match := OPINION_MD_H1.search(text):
            return match.group("label")
        return None

    @classmethod
    def key_from_md_prefix(cls, prefix: str) -> str | None:
        """Given a prefix containing a filename, e.g. `/hello/test/ponencia.md`,
        get the identifying key of the filename, e.g. `ponencia`."""
        if "/" in prefix and prefix.endswith(".md"):
            return prefix.split("/")[-1].split(".")[0]
        return None

    @classmethod
    def make_opinion(
        cls,
        path: str,
        decision_id: str,
        justice_id: int | None,
        text: str,
    ):
        """Common opinion instantiator for both `cls.from_folder()` and
        `cls.from_storage()`

        The `path` field implies that this may be a ponencia / opinion field.
        Ponencias are labeled 'ponencia.md' while Opinions are labeled
        '<digit>.md' where the digit refers to the justice id (representing
        the Justice) that penned the opinion.

        The `justice_id` refers to upstream value previously acquired for
        the ponencia. If the path's key refers to 'ponencia', then this
        `justice_id` value is utilized as the writer id; otherwise, use
        the <digit>.

        Each opinion consists of `segments`, `citations`, and `statutes`.
        """
        if key := cls.key_from_md_prefix(path):
            justice_id = justice_id if key == "ponencia" else int(key)
            opinion_id = f"{decision_id}-{key}"
            return cls(
                id=opinion_id,
                decision_id=decision_id,
                title=cls.get_headline(text),
                text=text,
                justice_id=justice_id,
                citations=list(Citation.extract_citations(text=text)),
                statutes=list(MentionedStatute.set_counted_statute(text=text)),
                segments=list(
                    OpinionSegment.make_segments(
                        decision_id=decision_id,
                        opinion_id=opinion_id,
                        text=text,
                    )
                ),
            )
        return None

    @classmethod
    def from_folder(
        cls,
        opinions_folder: Path,
        decision_id: str,
        ponente_id: int | None = None,
    ):
        """Assumes local folder containing opinions in .md format.
        The `ponente_id`, if present, will be used to populate the ponencia
        opinion."""
        for opinion_path in opinions_folder.glob("*.md"):
            if opinion := cls.make_opinion(
                path=str(opinion_path),
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
            Bucket=DECISION_BUCKET_NAME,
            Delimiter="/",
            Prefix=opinion_prefix,
        )
        for content in result["Contents"]:
            if not content["Key"].endswith(".md"):
                logger.error(f"Non .md {content['Key']=} in {opinion_prefix=}")
                continue

            text = decision_storage.restore_temp_txt(content["Key"])
            if not text:
                logger.error(f"No text restored from {content['Key']=}")
                continue

            opinion = cls.make_opinion(
                path=str(content["Key"]),
                decision_id=decision_id,
                justice_id=ponente_id,
                text=text,
            )
            if not opinion:
                logger.error(f"No opinion from {content['Key']=}")
                continue

            yield opinion
