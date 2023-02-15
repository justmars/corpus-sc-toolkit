import datetime
from typing import Any

from dateutil.parser import parse
from loguru import logger
from sqlite_utils.db import Database

from corpus_sc_toolkit.meta import (
    CourtComposition,
    DecisionCategory,
    DecisionSource,
    get_cite_from_fields,
    get_id_from_citation,
)

from .models import InterimDecision, InterimOpinion


def decision_from_pdf_db(
    db: Database, row: dict[str, Any]
) -> InterimDecision | None:
    """An `Interim Decision`'s fields will ultimately
    map out to a DecisionRow instance, a third-party library.

    The `row` described here is based on an sql exression:

    Args:
        db (Database): sqlite_utils.db wrapper over sqlite3
        row (dict[str, Any]): A matching row based on the sql expression above

    Returns:
        InterimDecision | None: If relevant fields are present, produce an instance of
            an InterimDecision
    """
    if not (cite := get_cite_from_fields(row)):
        logger.error(f"Bad citation in {row['id']=}")
        return None

    opx = InterimOpinion.setup(db, row)
    if not opx or not opx.get("opinions"):
        logger.error(f"No opinions detected in {row['id']=}")
        return None

    id = get_id_from_citation(
        folder_name=row["id"],
        source=DecisionSource.sc.value,
        citation=cite,
    )

    cat = DecisionCategory.set_category(
        category=row.get("category"),
        notice=row.get("notice"),
    )

    return InterimDecision(
        id=id,
        origin=row["id"],
        case_title=row["title"],
        created=datetime.datetime.now().timestamp(),
        modified=datetime.datetime.now().timestamp(),
        date_prom=parse(row["date"]).date(),
        date_scraped=parse(row["scraped"]).date(),
        citation=cite,
        composition=CourtComposition._setter(text=row["composition"]),
        category=cat,
        opinions=opx["opinions"],
        raw_ponente=opx.get("raw_ponente", None),
        per_curiam=opx.get("per_curiam", False),
        justice_id=opx.get("justice_id", None),
        is_pdf=True,
        emails=["bot@lawsql.com"],
    )
