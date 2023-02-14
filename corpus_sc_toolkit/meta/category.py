import re
from enum import Enum

CATEGORY_START_DECISION = re.compile(r"d\s*e\s*c", re.I)
CATEGORY_START_RESOLUTION = re.compile(r"r\s*e\s*s", re.I)


class DecisionCategory(str, Enum):
    """Each Decision (which is not classified as a 'notice of a minute resolution')
    is categorized as being either a `Decision` or a `Resolution`.

    A minute resolution, as described by the internal rules of the Philippine
    Supreme Court, is characterized as follows:

    > A notice of a minute resolution shall be embodied in a  letter of the Clerk
    of Court or the Division Clerk of Court notifying the parties of the action or
    actions taken in their case. In the absence of or whenever so deputized by the
    Clerk of Court or the Division Clerk of Court, the Assistant Clerk of Court or
    Assistant Division Clerk of Court may likewise sign the letter which shall
    be in the following form:

    > x x x

    > Sirs/Mesdames:

    > NOTICE

    > Please take notice that the Court ... x x x
    """

    decision = "Decision"
    resolution = "Resolution"
    minute = "Minute Resolution"
    other = "Unspecified"

    @classmethod
    def _setter(cls, text: str | None):
        """Detect pattern based on simple matching of characters.

        Examples:
            >>> text = "R E S O L U T I O N"
            >>> DecisionCategory._setter(text)
            <DecisionCategory.resolution: 'Resolution'>
            >>> text2 = "Decission" # wrongly spelled
            >>> DecisionCategory._setter(text2)
            <DecisionCategory.decision: 'Decision'>
        """
        if text:
            if CATEGORY_START_DECISION.search(text):
                return cls.decision
            elif CATEGORY_START_RESOLUTION.search(text):
                return cls.resolution
        return cls.other

    @classmethod
    def set_category(cls, category: str | None = None, notice: int | None = 0):
        if notice:
            return cls.minute
        if category:
            cls._setter(category)
        return cls.other
