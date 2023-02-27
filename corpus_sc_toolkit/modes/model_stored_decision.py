from collections.abc import Iterator
from typing import Any, Self
from loguru import logger
from sqlite_utils import Database
from pydantic import Field
from citation_utils import Citation
from corpus_sc_toolkit.justice import CandidateJustice
from corpus_sc_toolkit.meta import (
    CourtComposition,
    DecisionCategory,
    voteline_clean,
)

from .resources import (
    CLIENT,
    bucket_name,
    DOCKETS,
    YEARS,
    get_headline,
    DecisionFields,
    DecisionOpinion,
)


class StoredDecision(DecisionFields):
    """A decision may have previously been stored in an R2 storage instance.
    This facilitates the recall of such data.
    """

    opinions: list[DecisionOpinion] = Field(default_factory=list)

    @classmethod
    def get_opinions(cls, base_prefix: str) -> Iterator[dict[str, Any]]:
        """A part of the `get_content()` formula, it uses the same
        prefix to extract a "subfolder" of the bucket and subsequently
        place each  in a `dict`."""

        result = CLIENT.list_objects_v2(
            Bucket=bucket_name, Delimiter="/", Prefix=f"{base_prefix}opinions/"
        )
        for content in result["Contents"]:
            if content["Key"].endswith(".md"):
                op_key = cls.key_from_md_prefix(content["Key"])
                if text := cls.tmp_load(content["Key"], ext="md"):
                    if isinstance(text, str):
                        title = get_headline(text)
                        opx = {"op_key": op_key, "title": title, "text": text}
                        yield opx

    @classmethod
    def get_detailed_opinions_from_storage(cls, prefix: str) -> dict[str, Any]:
        """Used in tandem with `fetch()`, this extracts the `details.yaml` and
        `opinions/<key>.md` files into a single record based on a prefix which
        presumably is based on a `DecisionFields.base_prefix` formula."""
        key = f"{prefix}details.yaml"
        res = CLIENT.get_object(Bucket=bucket_name, Key=key)
        data = cls.tmp_load(src=key, ext="yaml")
        if not isinstance(data, dict):
            raise Exception(f"Bad details.yaml from {prefix=}")
        return data | {
            "id": cls.set_id(prefix),
            "opinions": list(cls.get_opinions(prefix)),
            "created": float(res["Metadata"]["time_created"]),
            "modified": float(res["Metadata"]["time_modified"]),
        }

    @classmethod
    def prefetch(
        cls, dockets: list[str] = DOCKETS, years: tuple[int, int] = YEARS
    ) -> Iterator[dict]:
        """Using prefixes from `iter_collections`, the results from R2 storage
        can be filtered based on `dockets` and `years`.

        Each result can then be used to get the main `details.yaml` object,
        download the same, and convert the download
        into a dict record.

        Since the fetched item is not yet complete, the method name is `prefetch`.
        """
        for collection in cls.iter_collections(dockets, years):
            for docket in collection["CommonPrefixes"]:
                yield cls.get_detailed_opinions_from_storage(docket["Prefix"])

    @classmethod
    def make(cls, r2_data: dict, db: Database) -> Self | None:
        """Using a single `r2_data` dict from `get_detailed_opinions_from_storage()`
        call, match justice data from the `db`, to get the proper justice id for opinion
        which will (finally) enable the construction of a single `StoredDecision`
        instance."""
        ponente = CandidateJustice(
            db=db,
            text=r2_data.get("ponente"),
            date_str=r2_data.get("date_prom"),
        )

        if not (cite := Citation.extract_citation_from_data(r2_data)):
            logger.error(f"Bad citation in {r2_data['id']=}")
            return None

        return cls(
            **ponente.ponencia,
            created=r2_data["created"],
            modified=r2_data["modified"],
            origin=r2_data["origin"],
            title=r2_data["case_title"],
            description=cite.display,
            date=r2_data["date_prom"],
            date_scraped=r2_data["date_scraped"],
            fallo=None,
            voting=voteline_clean(r2_data.get("voting")),
            citation=cite,
            emails=r2_data.get("emails", ["bot@lawsql.com"]),
            composition=CourtComposition._setter(r2_data.get("composition")),
            category=DecisionCategory._setter(r2_data.get("category")),
            opinions=[
                DecisionOpinion(
                    id=f"{r2_data['id']}-{op['op_key']}",
                    decision_id=r2_data["id"],
                    title=op["title"],
                    text=op["text"],
                    tags=[],
                    justice_id=(
                        ponente.id
                        if op["op_key"] == "ponencia"
                        else op["op_key"]
                    ),
                )
                for op in r2_data["opinions"]
            ],
        )
