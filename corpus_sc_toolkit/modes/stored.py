import yaml
from pathlib import Path
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
from markdownify import markdownify

from ._resources import (
    DOCKETS,
    YEARS,
    DecisionFields,
    DecisionOpinion,
    tmp_load,
)


class StoredDecision(DecisionFields):
    opinions: list[DecisionOpinion] = Field(default_factory=list)

    @classmethod
    def local(cls, path: Path, db: Database):
        """Assumes a decision will be loaded from a local `details.yaml` file
        with the following directory structure:

        ```sh
        ├── /decisions
        │   ├── /sc # from the supreme court e-library
        │   │   ├── /folder_name, e.g. 12341 # the original id when scraped
        │   │       ├── /details.yaml # the file containing the metadata that is `p`
        │   ├── /legacy
        │   │   ├── /folder_name, e.g. legacy-idfs2 # the original id when scraped

        ```

        The path `p` will have the following properties:

        1. `parent.name` = name of the parent folder, e.g. _12341_ or _legacy-idfs2_ above
        2. `p.parent.parent.stem` = name of the grandparent folder, e.g. _sc_ or _legacy_

        The properties will be combined with the `citation` extracted from the
        the data to form a [unique slug][set-decision-id-from-values]:

        In terms of what's found in the `/folder_name`, the directory may contain some
        html files, e.g.:

        1. `fallo.html`
        2. `ponencia.html`
        3. `annex.html`

        These may be utilized later in [DecisionHTMLConvertMarkdown][combine-html-files-of-e-lib-ponencia-to-markdown]

        The database `db` is relevant for purposes of determining the correct
        [justice][justice] to include as the ponente of the decision.

        """  # noqa: E501
        f = path.parent / "fallo.html"
        data = yaml.safe_load(path.read_text())
        if not (cite := Citation.extract_citation_from_data(data)):
            logger.error(f"Bad citation in {path=}")
            return None

        return cls(
            origin=path.parent.name,
            title=data.get("case_title"),
            description=cite.display,
            date=data.get("date_prom"),
            date_scraped=data.get("date_scraped"),
            composition=CourtComposition._setter(data.get("composition")),
            category=DecisionCategory._setter(data.get("category")),
            fallo=markdownify(f.read_text()) if f.exists() else None,
            voting=voteline_clean(data.get("voting")),
            citation=cite,
            emails=data.get("emails", ["bot@lawsql.com"]),
            **CandidateJustice(
                db=db,
                text=data.get("ponente"),
                date_str=data.get("date_prom"),
            ).ponencia,
        )

    @classmethod
    def preget(cls, prefix: str) -> dict[str, Any]:
        """Used in tandem with `prefetch()`, this extracts key-value pairs
        from the prefix ending with `/details.yaml`. Note that what is returned
        is a `dict` instance rather than a `Stored Decision`. This is because
        it's still missing fields that can only be supplied by using a database.

        Args:
            prefix (str): Must end with `/details.yaml`

        Returns:
            dict[str, Any]: Identified dict from R2 containing the details.yaml prefix.
        """
        if not prefix.endswith("/details.yaml"):
            raise Exception("Method limited to details.yaml.")
        candidate = prefix.removesuffix("/details.yaml")
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
        for collection in cls.iter_collections(dockets, years):
            prefixed_dockets = collection["CommonPrefixes"]
            for docket in prefixed_dockets:
                prefix = f'{docket["Prefix"]}details.yaml'
                yield cls.preget(prefix)

    @classmethod
    def make(cls, r2_data: dict, db: Database) -> Self | None:
        """Using a single `r2_data` dict from a `preget()` call, match justice data
        from the `db`, to get the proper justice id for opinion which will (finally)
        enable the construction of a single `StoredDecision` instance."""
        ponente = CandidateJustice(
            db=db,
            text=r2_data.get("ponente"),
            date_str=r2_data.get("date_prom"),
        )

        opinions = list(
            DecisionOpinion.fetch(
                base_prefix=r2_data["prefix"],
                decision_id=r2_data["id"],
                ponente_id=ponente.id,
            )
        )
        if not opinions:
            logger.error(f"No opinions detected in {r2_data['id']=}")
            return None

        if not (cite := Citation.extract_citation_from_data(r2_data)):
            logger.error(f"Bad citation in {r2_data['id']=}")
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
