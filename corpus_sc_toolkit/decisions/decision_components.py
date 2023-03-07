import re
from collections.abc import Iterator
from typing import Self

from pydantic import BaseModel, Field
from statute_patterns import count_rules
from statute_trees import StatuteBase


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
