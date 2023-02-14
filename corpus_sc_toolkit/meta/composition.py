import re
from enum import Enum


class CourtComposition(str, Enum):
    """The Supreme Court may sit either `en banc` (the full 15 member complement)
    or by `division` (5-member groups).
    """

    enbanc = "En Banc"
    division = "Division"
    other = "Unspecified"

    @classmethod
    def _setter(cls, text: str | None):
        """Detect pattern based on simple matching of characters.

        Examples:
            >>> text = "En Banc"
            >>> CourtComposition._setter(text)
            <CourtComposition.enbanc: 'En Banc'>
            >>> text2 = "Special First Division"
            >>> CourtComposition._setter(text2)
            <CourtComposition.division: 'Division'>
        """
        if text:
            if "banc".casefold() in text.casefold():
                return cls.enbanc
            elif "div".casefold() in text.casefold():
                return cls.division
        return cls.other
