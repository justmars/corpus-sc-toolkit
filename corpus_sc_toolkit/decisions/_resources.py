import datetime
from pathlib import Path

from start_sdk.cf_r2 import StorageUtils

DECISION_TEMP_FOLDER = Path(__file__).parent / "_tmp"
DECISION_TEMP_FOLDER.mkdir(exist_ok=True)

DECISION_BUCKET_NAME = "sc-decisions"
decision_storage = StorageUtils(
    name=DECISION_BUCKET_NAME, temp_folder=DECISION_TEMP_FOLDER
)
meta = decision_storage.resource.meta
if not meta:
    raise Exception("Bad bucket.")
DECISION_CLIENT = meta.client

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
