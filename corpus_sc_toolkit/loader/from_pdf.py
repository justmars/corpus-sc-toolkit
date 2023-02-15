from typing import Any
from corpus_sc_toolkit.meta import (
    get_id_from_citation,
    get_cite_from_fields,
    DecisionSource,
    CourtComposition,
    DecisionCategory,
)
from dateutil.parser import parse
from .interim_models import InterimDecision, InterimOpinion
from corpus_sc_toolkit.resources import SC_BASE_URL
from sqlite_utils.db import Database
from loguru import logger


def decision_from_pdf_db(
    db: Database, row: dict[str, Any]
) -> InterimDecision | None:
    """An `Interim Decision`'s fields will ultimately
    map out to a DecisionRow instance, a third-party library.

    The `row` described here is based on an sql exression:

    ```sql
    WITH opinions_included AS (
    SELECT
        op.id,
        op.pdf,
        op.title,
        op_meta.writer,
        op_meta.body opinion_body,
        op_meta.annex opinion_annex
    FROM
        pre_tbl_opinions op
        JOIN pre_tbl_opinion_meta op_meta
        ON op_meta.opinion_id = op.id
    WHERE
        op.category = caso.category
        AND op.serial = caso.serial
        AND op.date = caso.date
    ),
    opinion_list_data AS (
    SELECT
        json_group_array(
        json_object(
            'id',
            op_inc.id,
            'pdf',
            op_inc.pdf,
            'title',
            op_inc.title,
            'writer',
            op_inc.writer,
            'body',
            op_inc.opinion_body,
            'annex',
            op_inc.opinion_annex
        )
        ) opinion_list
    FROM
        opinions_included op_inc
    ),
    opinions_with_ponencia AS (
    SELECT
        json_insert(
        (
            SELECT
            opinion_list
            FROM
            opinion_list_data
        ),
        '$[#]',
        json_object(
            'id',
            caso.id,
            'pdf',
            caso.pdf,
            'title',
            CASE meta.notice
            WHEN 1 THEN 'Notice'
            WHEN 0 THEN 'Ponencia'
            END,
            'writer',
            meta.writer,
            'body',
            meta.body,
            'annex',
            meta.annex
        )
        ) opinions
    )
    SELECT
    caso.scraped,
    caso.id,
    caso.title,
    caso.category docket_category,
    caso.serial,
    caso.date,
    caso.pdf,
    meta.composition,
    meta.notice,
    meta.category,
    (
        SELECT
        opinions
        FROM
        opinions_with_ponencia
    ) opinions
    FROM
        pre_tbl_decisions caso
        JOIN pre_tbl_decision_meta meta
        ON meta.decision_id = caso.id
    WHERE
        meta.notice = 0
    ```

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
        origin=f"{SC_BASE_URL}/{row['id']}",
        case_title=row["title"],
        date_prom=parse(row["date"]).date(),
        date_scraped=parse(row["scraped"]).date(),
        citation=cite,
        composition=CourtComposition._setter(text=row["composition"]),
        category=cat,
        opinions=opx["opinions"],
        raw_ponente=opx.get("raw_ponente", None),
        per_curiam=opx.get("per_curiam", False),
        justice_id=opx.get("justice_id", None),
    )
