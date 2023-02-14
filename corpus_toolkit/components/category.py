import re
from enum import Enum

CATEGORY_START_DECISION = re.compile(r"d\s*e\s*c", re.I)
CATEGORY_START_RESOLUTION = re.compile(r"r\s*e\s*s", re.I)


class DecisionCategory(str, Enum):
    decision = "Decision"
    resolution = "Resolution"
    other = "Unspecified"

    @classmethod
    def _setter(cls, text: str | None):
        if text:
            if CATEGORY_START_DECISION.search(text):
                return cls.decision
            elif CATEGORY_START_RESOLUTION.search(text):
                return cls.resolution
        return cls.other

    @classmethod
    def set_category(cls, category: str | None = None, notice: int | None = 0):
        if notice:
            return cls.resolution
        if category:
            cls._setter(category)
        return cls.other
