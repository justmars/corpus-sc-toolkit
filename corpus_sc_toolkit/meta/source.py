from enum import Enum


class DecisionSource(str, Enum):
    """The Supreme Court website contains decisions starting from 1996 onwards.
    Decisions dated prior to that year are only available through various secondary
    sources.

    For purposes of classification and determining their provenance, we will use the
    following categorization:

    source | description
    --:|:--
    sc | 1996 onwards from the Supreme Court
    legacy | Prior to 1996, from other sources
    """

    sc = "sc"
    legacy = "legacy"
