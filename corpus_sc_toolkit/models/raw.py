from collections.abc import Iterator
from typing import Any, Self

from citation_utils import Citation
from loguru import logger
from sqlite_utils import Database

from corpus_sc_toolkit.justice import CandidateJustice
from corpus_sc_toolkit.meta import (
    CourtComposition,
    DecisionCategory,
    voteline_clean,
)

from ._resources import (
    DETAILS_FILE,
    DOCKETS,
    SUFFIX_OPINION,
    YEARS,
    DecisionFields,
    DecisionOpinion,
    tmp_load,
)


class RawDecision(DecisionFields):
    ...

    @classmethod
    def preget(cls, prefix: str) -> dict[str, Any]:
        """Used in tandem with `prefetch()`, this extracts key-value pairs
        from the prefix ending with `/details.yaml`. Note that what is returned
        is a `dict` instance rather than a `Raw Decision`. This is because
        it's still missing fields that can only be supplied by using a database.

        Args:
            prefix (str): Must end with `/details.yaml`

        Returns:
            dict[str, Any]: Identified dict from R2 containing the details.yaml prefix.
        """
        if not prefix.endswith(f"/{DETAILS_FILE}"):
            raise Exception("Method limited to details.yaml.")
        candidate = prefix.removesuffix(f"/{DETAILS_FILE}")
        identity = {"prefix": candidate, "id": cls.set_id(candidate)}
        data = tmp_load(src=prefix, ext="yaml")
        if not isinstance(data, dict):
            raise Exception(f"Bad details.yaml from {prefix=}")
        return identity | data

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
            yield cls.preget(f"{prefix}{DETAILS_FILE}")

    @classmethod
    def make(cls, r2_data: dict, db: Database) -> Self | None:
        """Using a single `r2_data` dict from a `preget()` call, match justice data
        from the `db`. This enables construction of a single `RawDecision` instance.
        """
        if not (cite := Citation.extract_citation_from_data(r2_data)):
            logger.error(f"Bad citation in {r2_data['id']=}")
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
            **ponente.ponencia,
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
        )
