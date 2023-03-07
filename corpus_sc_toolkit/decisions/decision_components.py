import re
from collections.abc import Iterator
from enum import Enum
from pathlib import Path
from typing import Self

from citation_utils import Citation
from loguru import logger
from pydantic import BaseModel, Field
from statute_patterns import count_rules
from statute_trees import StatuteBase

from ._resources import DECISION_BUCKET_NAME, DECISION_CLIENT, decision_storage


def standardize(text: str):
    return (
        text.replace("\xa0", "")
        .replace("\xad", "-")
        .replace("“", '"')
        .replace("”", '"')
        .replace("‘", "'")
        .replace("’", "'")
        .strip()
    )


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


single_spaced = re.compile(r"\s*\n\s*")
double_spaced = re.compile(r"\s*\n\s*\n\s*")


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
    def segmentize(
        cls, full_text: str, min_num_chars: int = 10
    ) -> Iterator[dict]:
        """Split first by double-spaced breaks `\\n\\n` and then by
        single spaced breaks `\\n` to get the position of the segment.

        Will exclude footnotes and segments with less than 10 characters.

        Args:
            full_text (str): The opinion to segment

        Yields:
            Iterator[dict]: The partial segment data fields
        """
        if cleaned_text := standardize(full_text):
            if subdivisions := double_spaced.split(cleaned_text):
                for idx, text in enumerate(subdivisions):
                    if lines := single_spaced.split(text):
                        for sub_idx, segment in enumerate(lines):
                            # --- marks the footnote boundary in # converter.py
                            if segment == "---":
                                return
                            position = f"{idx}-{sub_idx}"
                            char_count = len(segment)
                            if char_count > min_num_chars:
                                yield {
                                    "position": position,
                                    "segment": segment,
                                    "char_count": char_count,
                                }

    @classmethod
    def make_segments(
        cls, decision_id: str, opinion_id: str, text: str
    ) -> Iterator[Self]:
        """Auto-generated segments based on the text of the opinion."""
        for extract in cls.segmentize(text):
            yield cls(
                id=f"{opinion_id}-{extract['position']}",
                decision_id=decision_id,
                opinion_id=opinion_id,
                **extract,
            )


OPINION_MD_H1 = re.compile(r"^#\s*(?P<label>.*)")


class OpinionTag(str, Enum):
    ponencia = "Ponencia"
    concur = "Concurring Opinion"
    dissent = "Dissenting Opinion"
    separate = "Separate Opinion"

    @classmethod
    def detect(cls, text: str):
        tags = []
        for name, member in cls.__members__.items():
            if name in text.lower():
                tags.append(member)
        return tags


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
    tags: list[OpinionTag] | None = Field(
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

    class Config:
        use_enum_values = True

    def make_filename_for_upload(self):
        if self.title == "Ponencia":
            return "ponencia.md"
        elif self.justice_id:
            return f"{self.justice_id}.md"
        logger.warning(f"No filename for {self.id=} {self.decision_id=}")
        return None

    def to_storage(self, decision_prefix: str):
        logger.debug(f"Uploading opinion {self.id=}")
        prefix_title = self.make_filename_for_upload()
        if not prefix_title:
            logger.warning("Missing title, skip upload.")
            return None
        temp_md = Path(__file__).parent / "temp.md"
        temp_md.write_text(self.text)
        decision_storage.upload(
            file_like=temp_md,
            loc=f"{decision_prefix}/opinions/{prefix_title}",
            args=decision_storage.set_extra_meta(self.storage_meta),
        )
        temp_md.unlink(missing_ok=True)

    @property
    def storage_meta(self):
        if not self.title or not self.justice_id:
            return {}
        return {
            "id": self.id,
            "title": self.title,
            "tags": ",".join([t for t in self.tags]) if self.tags else None,
            "justice_id": self.justice_id,
            "pdf": self.pdf,
            "text_length": len(self.text),
            "num_unique_statutes": len(self.statutes),
            "num_detected_citations": len(self.citations),
            "num_counted_segments": len(self.segments),
        }

    @classmethod
    def get_headline(cls, text: str) -> str | None:
        """Markdown contains H1 header, extract this header."""
        if match := OPINION_MD_H1.search(text):
            label = match.group("label")
            if len(label) >= 5 and len(label) <= 50:
                return label
            else:
                logger.error(f"Improper opinion {label=} regex capture.")
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
        key = cls.key_from_md_prefix(path)
        if not key:
            logger.error(f"No key from {path=}")
            return None

        title = cls.get_headline(text)
        if not title:
            logger.error(f"No headline from {path=}; means no title.")
            return None

        justice_id = justice_id if key == "ponencia" else int(key)
        opinion_id = f"{decision_id}-{key}"
        return cls(
            id=opinion_id,
            decision_id=decision_id,
            title=title,
            text=text,
            justice_id=justice_id,
            tags=OpinionTag.detect(title),
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
        for opinion_path in opinions_folder.glob("**/*.md"):
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
