import re
import yaml
import datetime
from collections.abc import Iterator
from typing import Any
from pathlib import Path
from start_sdk import CFR2_Bucket
from loguru import logger
from pydantic import Field
from citation_utils import Citation
from corpus_sc_toolkit.justice import CandidateJustice
from corpus_sc_toolkit.meta import (
    CourtComposition,
    DecisionCategory,
    voteline_clean,
)
from sqlite_utils import Database

from .fields import DecisionFields, DecisionOpinion


TEMP_FOLDER = Path(__file__).parent.parent / "tmp"
TEMP_FOLDER.mkdir(exist_ok=True)

bucket_name = "sc-decisions"
origin = CFR2_Bucket(name=bucket_name)
meta = origin.resource.meta
if not meta:
    raise Exception("Bad bucket.")

client = meta.client
bucket = origin.bucket
dockets: list[str] = ["GR", "AM", "OCA", "AC", "BM"]
years: tuple[int, int] = (1902, datetime.datetime.now().date().year)
months = range(1, 13)


def get_dated_prefixes(
    dockets: list[str] = dockets, years: tuple[int, int] = years
) -> Iterator[str]:
    """Results in the following prefix format: `<docket>/<year>/<month>`
    in ascending order."""
    for docket in dockets:
        cnt_year, end_year = years[0], years[1]
        while cnt_year <= end_year:
            for month in months:
                yield f"{docket}/{cnt_year}/{month}/"
            cnt_year += 1


def iter_collections(
    dockets: list[str] = dockets, years: tuple[int, int] = years
) -> Iterator[dict[str, Any]]:
    """Based on a list of prefixes ordered by date, get the list of objects
    per prefix. Each item in the collection is a dict which will contain
    a `CommonPrefixes` key."""
    for prefix in get_dated_prefixes(dockets, years):
        yield client.list_objects_v2(
            Bucket=bucket_name, Delimiter="/", Prefix=prefix
        )


def tmp_load(src: str, ext: str = "yaml") -> str | dict[str, Any] | None:
    """Based on the `src` prefix, download the same into a temp file
    and return its contents based on the extension. A `yaml` extension
    should result in contents in `dict` format; where an `md` or `html`
    extension results in `str`. The temp file is deleted after every
    successful extraction of the `src` as content."""
    path = TEMP_FOLDER / f"temp.{ext}"
    origin.download(src, str(path))
    content = None
    if ext == "yaml":
        content = yaml.safe_load(path.read_bytes())
    elif ext in ["md", "html"]:
        content = path.read_text()
    path.unlink(missing_ok=True)
    return content


def set_id(prefix: str):
    return prefix.removesuffix("/").replace("/", "-").lower()


headline = re.compile(r"^#\s*(?P<label>).*$")


def get_opinions(base_prefix: str) -> Iterator[dict[str, Any]]:
    """A part of the `get_content()` formula, it uses the same
    prefix to extract a "subfolder" of the bucket and subsequently
    place each  in a `dict`."""

    result = client.list_objects_v2(
        Bucket=bucket_name, Delimiter="/", Prefix=f"{base_prefix}opinions/"
    )
    for content in result["Contents"]:
        if content["Key"].endswith(".md"):
            if text := tmp_load(content["Key"], ext="md"):
                if isinstance(text, str):
                    yield {
                        "id": content["Key"].split("/")[-1].split(".")[0],
                        "title": m.group("label")
                        if (m := headline.search(text))
                        else "Not Found",  # noqa: E501
                        "text": text,
                    }


def get_content(prefix: str) -> dict[str, Any]:
    key = f"{prefix}details.yaml"
    res = client.get_object(Bucket=bucket_name, Key=key)
    data = tmp_load(src=key, ext="yaml")
    if not isinstance(data, dict):
        raise Exception(f"Bad details.yaml from {prefix=}")
    return data | {
        "id": set_id(prefix),
        "opinions": list(get_opinions(prefix)),
        "created": float(res["Metadata"]["time_created"]),
        "modified": float(res["Metadata"]["time_modified"]),
    }


def get_contents(
    dockets: list[str] = dockets, years: tuple[int, int] = years
) -> Iterator[dict]:
    for collection in iter_collections(dockets, years):
        for docket in collection["CommonPrefixes"]:
            yield get_content(docket["Prefix"])


class StoredDecision(DecisionFields):
    opinions: list[DecisionOpinion] = Field(default_factory=list)

    @classmethod
    def _set(cls, raw: dict, db: Database):
        ponente = CandidateJustice(
            db=db, text=raw.get("ponente"), date_str=raw.get("date_prom")
        )
        if not (cite := Citation.extract_citation_from_data(raw)):
            logger.error(f"Bad citation in {raw['id']=}")
            return None
        return cls(
            **ponente.ponencia,
            id=raw["id"],
            created=raw["created"],
            modified=raw["modified"],
            origin=raw["origin"],
            title=raw["case_title"],
            description=cite.display,
            date=raw["date_prom"],
            date_scraped=raw["date_scraped"],
            composition=CourtComposition._setter(raw.get("composition")),
            category=DecisionCategory._setter(raw.get("category")),
            fallo=None,
            voting=voteline_clean(raw.get("voting")),
            citation=cite,
            emails=raw.get("emails", ["bot@lawsql.com"]),
            opinions=[
                DecisionOpinion(
                    id=f"{raw['id']}-{op['id']}",
                    decision_id=raw["id"],
                    title=op["title"],
                    text=op["text"],
                    tags=[],
                    justice_id=ponente.id
                    if op["id"] == "ponencia"
                    else op["id"],
                )
                for op in raw["opinions"]
            ],
        )
