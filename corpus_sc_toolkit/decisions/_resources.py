import datetime

from start_sdk import CFR2_Bucket

BUCKET_NAME = "sc-decisions"
ORIGIN = CFR2_Bucket(name=BUCKET_NAME)
meta = ORIGIN.resource.meta
if not meta:
    raise Exception("Bad bucket.")
CLIENT = meta.client
"""R2 variables in order to perform operations from the library."""


"""Decision structure aspects."""

DOCKETS: list[str] = ["GR", "AM", "OCA", "AC", "BM"]
"""Default selection of docket types to serve as root prefixes in R2."""

SC_START_YEAR = 1902
PRESENT_YEAR = datetime.datetime.now().date().year
YEARS: tuple[int, int] = (SC_START_YEAR, PRESENT_YEAR)
"""Default range of years to serve as prefixes in R2"""


DETAILS_FILE = "details.yaml"
"""Note that this does not have a backslash"""

PDF_FILE = "pdf.yaml"
"""Note that this does not have a backslash"""

SUFFIX_PDF = f"/{PDF_FILE}"
"""Note inclusion of start backslash"""

SUFFIX_OPINION = "/opinions/"
"""Note inclusion of start and end backslashes"""
