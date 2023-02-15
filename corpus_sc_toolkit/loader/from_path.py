from pathlib import Path

import yaml
from citation_utils import Citation
from loguru import logger
from markdownify import markdownify
from sqlite_utils import Database

from corpus_sc_toolkit.justice import CandidateJustice
from corpus_sc_toolkit.meta import (
    CourtComposition,
    DecisionCategory,
    DecisionSource,
    get_id_from_citation,
    voteline_clean,
)

from .models import BaseDecision


def decision_from_path(p: Path, db: Database) -> BaseDecision | None:
    """The BaseDecision fields that are created through this
    function will ultimately map out to a DecisionRow instance, a third-party library.

    It consolidates the various [metadata][meta] from the toolkit.

    It assumes that a decision will be loaded from a `details.yaml` file
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
    f = p.parent / "fallo.html"
    data = yaml.safe_load(p.read_text())
    if not (cite := Citation.extract_citation_from_data(data)):
        logger.error(f"Bad citation in {p=}")
        return None

    return BaseDecision(
        id=get_id_from_citation(
            folder_name=p.parent.name,
            source=p.parent.parent.stem,
            citation=cite,
        ),
        created=p.stat().st_ctime,
        modified=p.stat().st_mtime,
        source=DecisionSource(p.parent.parent.stem),
        origin=p.parent.name,
        title=data.get("case_title"),
        description=cite.display,
        date=data.get("date_prom"),
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
