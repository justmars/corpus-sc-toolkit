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


def list_decision_r2(*args, **kwargs):
    return DECISION_CLIENT.list_objects_v2(
        Bucket=DECISION_BUCKET_NAME, *args, **kwargs
    )


def get_decision_objects():
    contents = []
    counter = 1
    next_token = None
    while True:
        print(f"Accessing page {counter=}")
        if counter == 1:
            res = list_decision_r2()
        elif next_token:
            res = list_decision_r2(ContinuationToken=next_token)
        else:
            print("Missing next token.")
            break

        next_token = res.get("NextContinuationToken")
        if res.get("Contents"):
            contents.extend(res["Contents"])
        counter += 1
        if not res["IsTruncated"]:  # is False if all results returned.
            print("All results returned.")
            return contents


def filter_objects(filter_suffix: str, objects_list: list[dict]):
    for obj in objects_list:
        if key := obj.get("Key"):
            if key.endswith(filter_suffix):
                yield obj
