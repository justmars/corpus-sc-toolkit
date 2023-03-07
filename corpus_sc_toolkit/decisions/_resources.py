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
