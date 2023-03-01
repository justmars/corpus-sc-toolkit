from ._resources import (
    DOCKETS,
    YEARS,
    DecisionFields,
    DecisionOpinion,
    OpinionSegment,
)
from .interim import InterimDecision, InterimOpinion
from .raw import RawDecision
from .txt import (
    DecisionHTMLConvertMarkdown,
    add_markdown_file,
    segmentize,
    standardize,
)
