from collections.abc import Iterator
from typing import Any, Self

from citation_utils import Citation
from loguru import logger
from sqlite_utils import Database

from ._resources import (
    DETAILS_FILE,
    DOCKETS,
    SUFFIX_OPINION,
    YEARS,
    decision_storage,
)
from .decision_fields import DecisionFields
from .decision_substructures import DecisionOpinion
from .fields import (
    CourtComposition,
    DecisionCategory,
    voteline_clean,
)
from .justice import CandidateJustice


class RawDecision(DecisionFields):
    ...

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
            if result := decision_storage.restore_temp_yaml(
                yaml_suffix=target
            ):
                yield result

    @classmethod
    def make(cls, r2_data: dict, db: Database) -> Self | None:
        """Using a single `r2_data` dict from a `preget()` call, match justice data
        from the `db`. This enables construction of a single `RawDecision` instance.
        """

        cite = Citation.extract_citation_from_data(r2_data)
        if not cite:
            logger.error(f"Bad citation in {r2_data['id']=}")
            return None

        decision_id = cite.prefix_db_key
        if not decision_id:
            logger.error(f"Bad decision_id in {r2_data['id']=}")
            return None

        decision_prefix = cite.storage_prefix
        if not decision_prefix:
            logger.error(f"Bad decision_prefix in {r2_data['id']=}")
            return None

        ponente = CandidateJustice(
            db=db,
            text=r2_data.get("ponente"),
            date_str=r2_data.get("date_prom"),
        )

        opinions = list(
            DecisionOpinion.fetch(
                opinion_prefix=f"{r2_data['prefix']}{SUFFIX_OPINION}",
                decision_id=r2_data["id"],
                ponente_id=ponente.id,
            )
        )
        if not opinions:
            logger.error(f"No opinions detected in {r2_data['id']=}")
            return None

        return cls(
            id=decision_id,
            prefix=decision_prefix,
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
            opinions=opinions,
            **ponente.ponencia,
        )
