__version__ = "0.0.8"

from .justice import (
    CandidateJustice,
    Justice,
    JusticeDetail,
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
from .modes import (
    BaseDecision,
    DecisionHTMLConvertMarkdown,
    InterimDecision,
    InterimOpinion,
    add_markdown_file,
    decision_from_path,
    decision_from_pdf_db,
    segmentize,
    standardize,
)
from .resources import DECISION_PATH, SC_BASE_URL, SC_LOCAL_FOLDER
