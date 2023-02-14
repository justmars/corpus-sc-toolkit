import re
from enum import Enum

COMPOSITION_START_DIVISION = re.compile(r"div", re.I)
COMPOSITION_START_ENBANC = re.compile(r"en", re.I)


class CourtComposition(str, Enum):
    enbanc = "En Banc"
    division = "Division"
    other = "Unspecified"

    @classmethod
    def _setter(cls, text: str | None):
        if text:
            if COMPOSITION_START_DIVISION.search(text):
                return cls.division
            elif COMPOSITION_START_ENBANC.search(text):
                return cls.enbanc
        return cls.other
