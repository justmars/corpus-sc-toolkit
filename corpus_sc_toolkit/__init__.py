__version__ = "0.0.5"

from .html import (
    DecisionHTMLConvertMarkdown,
    add_markdown_file,
    segmentize,
    standardize,
)
from .justice import (
    CandidateJustice,
    Justice,
    OpinionWriterName,
    get_justices_file,
    get_justices_from_api,
)
from .meta import (
    CourtComposition,
    DecisionCategory,
    DecisionSource,
    extract_votelines,
    get_cite_from_fields,
    get_id_from_citation,
    tags_from_title,
    voteline_clean,
)
from .pdf import InterimDecision, InterimOpinion
from .resources import DECISION_PATH, SC_BASE_URL, SC_LOCAL_FOLDER
