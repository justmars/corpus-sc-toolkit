from collections.abc import Iterator
from pathlib import Path

import yaml
from citation_utils import Citation
from loguru import logger
from markdownify import markdownify
from pydantic import Field
from sqlite_utils import Database

from ._resources import DECISION_BUCKET_NAME, DECISION_CLIENT, decision_storage
from .decision_fields import DecisionFields
from .decision_opinions import DecisionOpinion
from .fields import (
    CourtComposition,
    DecisionCategory,
    voteline_clean,
)
from .justice import CandidateJustice

DETAILS_KEY = "details.yaml"
"""This suffix is uploaded / retrieved from R2 storage based on `DecisionHTML`."""


class DecisionHTML(DecisionFields):
    home_html: Path | None = Field(default=None, exclude=True)

    @classmethod
    def get_key(cls, dated_prefix: str) -> str | None:
        """Is suffix `details.yaml` present in result of `cls.iter_dockets()`?"""
        key = f"{dated_prefix}/{DETAILS_KEY}"
        try:
            DECISION_CLIENT.get_object(Bucket=DECISION_BUCKET_NAME, Key=key)
            return key
        except Exception:
            return None

    @classmethod
    def get_existing_prefixes(cls) -> Iterator[str]:
        if object_list := decision_storage.all_items():
            if filtered_list := decision_storage.filter_content(
                filter_suffix="/{DETAILS_KEY}", objects_list=object_list
            ):
                for item in filtered_list:
                    yield item["Key"]

    def to_storage(self):
        # Uses `details.yaml` to upload decision fields represented by instance.
        self.put_in_storage(DETAILS_KEY)

        # Upload legacy html files
        if self.home_html and self.home_html.exists():
            loc = f"{self.prefix}/body.html"
            decision_storage.upload(file_like=self.home_html, loc=loc)

        # Upload markdown-based opinion files
        for opinion in self.opinions:
            opinion.to_storage(self.prefix)

    @classmethod
    def get_common(
        cls, data: dict, db: Database
    ) -> tuple[str, str, Citation, CandidateJustice] | None:
        """Unpack initial identifying / complex values"""
        if not (res := Citation.extract_id_prefix_citation_from_data(data)):
            return None
        return *res, CandidateJustice(
            db=db, text=data.get("ponente"), date_str=data.get("date_prom")
        )

    @classmethod
    def make_from_path(cls, local_path: Path, db: Database):
        """Using local_path, match justice data containing a ponente field and a date
        from the `db`. This enables construction of a single `DecisionHTML` instance.
        """
        folder_path = local_path.parent

        fallo = None
        fallo_file = folder_path / "fallo.html"
        if fallo_file.exists():
            fallo = markdownify(fallo_file.read_text()).strip()

        local = yaml.safe_load(local_path.read_bytes())
        result = cls.get_common(local, db)
        if not result:
            return None

        decision_id, prefix, cite, ponente = result

        opinions_folder = folder_path / "opinions"
        if not opinions_folder.exists():
            logger.error(f"Missing {opinions_folder=}")
            return None

        opinions = list(
            DecisionOpinion.from_folder(  # from folder vs. from storage
                opinions_folder=opinions_folder,
                decision_id=decision_id,
                ponente_id=ponente.id,
            )
        )
        if not opinions:
            logger.error(f"No opinions in {opinions_folder=} {decision_id=}")
            return None

        decision = cls(
            id=decision_id,
            prefix=prefix,
            citation=cite,
            origin=local["origin"],
            title=local["case_title"],
            description=cite.display,
            date=local["date_prom"],
            date_scraped=local["date_scraped"],
            fallo=fallo,
            voting=voteline_clean(local.get("voting")),
            emails=local.get("emails", ["bot@lawsql.com"]),
            composition=CourtComposition._setter(local.get("composition")),
            category=DecisionCategory._setter(local.get("category")),
            opinions=opinions,
            **ponente.ponencia,
        )
        return decision
