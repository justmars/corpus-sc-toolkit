import re
from typing import NamedTuple

notice_letter_start = re.compile(
    r"""
    [.\n]*
    Sirs\/Mesdames
    [\s\S]+?
    as\s+follows:
""",
    re.I | re.X,
)


class Notice(NamedTuple):
    msg: str
    txt: str

    @classmethod
    def extract(cls, body_text: str):
        if match := notice_letter_start.search(body_text):
            e = match.span()[1]
            return cls(msg=body_text[:e], txt=body_text[e:])
        return None
