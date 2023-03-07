from collections.abc import Iterator
from pathlib import Path
from typing import Self

import yaml
from citation_utils import Citation
from loguru import logger
from markdownify import markdownify
from sqlite_utils import Database

from ._resources import (
    DETAILS_FILE,
    DOCKETS,
    SUFFIX_OPINION,
    YEARS,
    decision_storage,
)
from .decision_components import DecisionOpinion
from .decision_fields import DecisionFields
from .fields import (
    CourtComposition,
    DecisionCategory,
    voteline_clean,
)
from .justice import CandidateJustice


class DecisionHTML(DecisionFields):
    ...

    def upload(self):
        """Uses `details.yaml` to upload the entire set of decision fields
        represented by this instance."""
        self.put_in_storage(DETAILS_FILE)

    @classmethod
    def prefetch(
        cls, dockets: list[str] = DOCKETS, years: tuple[int, int] = YEARS
    ) -> Iterator[dict]:
        """Using prefixes from `iter_collections`, the results from R2 storage
        can be filtered based on `dockets` and `years`. Each result can then be
        used to get the main `details.yaml` object, download the same, and convert
        the download into a dict record. Since the fetched item is not yet complete,
        the method name is `prefetch`.

        Args:
            dockets (list[str], optional): Selection of docket types e.g. ["GR", "AM"].
                Defaults to DOCKETS.
            years (tuple[int, int], optional): Range of years e.g. (1996,1998).
                Defaults to YEARS.

        Yields:
            Iterator[dict]: Identified dicts from R2 containing details.yaml prefix.
        """
        for prefix in cls.iter_dockets(dockets, years):
            target = f"{prefix}{DETAILS_FILE}"
            result = decision_storage.restore_temp_yaml(yaml_suffix=target)
            if result:
                yield result

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
        fallo = None
        fallo_file = local_path.parent / "fallo.html"
        if fallo_file.exists():
            fallo = markdownify(fallo_file.read_text()).strip()

        local = yaml.safe_load(local_path.read_bytes())
        result = cls.get_common(local, db)
        if not result:
            return None

        decision_id, prefix, cite, ponente = result

        opinions_folder = local_path.parent / "opinions"
        if not opinions_folder.exists():
            logger.error(f"No opinions folder in {local_path=}")
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

        return cls(
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

    @classmethod
    def make_from_storage(cls, r2_data: dict, db: Database) -> Self | None:
        """Implies `r2_data` extraction from `decision_storage.restore_temp_yaml()`.
        Based on this result, match justice data from the `db`.
        This enables construction of a single `DecisionHTML` instance.
        """
        if not (extracted := cls.get_common(r2_data, db)):
            logger.error(f"Missing extraction {r2_data['prefix']=}")
            return None
        decision_id, prefix, cite, ponente = extracted

        opinions = list(
            DecisionOpinion.from_storage(  # from storage vs. from storage
                opinion_prefix=f"{r2_data['prefix']}{SUFFIX_OPINION}",
                decision_id=decision_id,
                ponente_id=ponente.id,
            )
        )
        if not opinions:
            logger.error(f"No opinions detected in {decision_id=}")
            return None

        return cls(
            id=decision_id,
            prefix=prefix,
            citation=cite,
            origin=r2_data["origin"],
            title=r2_data["case_title"],
            description=cite.display,
            date=r2_data["date_prom"],
            date_scraped=r2_data["date_scraped"],
            fallo=None,
            voting=voteline_clean(r2_data.get("voting")),
            emails=r2_data.get("emails", ["bot@lawsql.com"]),
            composition=CourtComposition._setter(r2_data.get("composition")),
            category=DecisionCategory._setter(r2_data.get("category")),
            opinions=opinions,
            **ponente.ponencia,
        )
