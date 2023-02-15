import datetime
from collections.abc import Iterator
from pathlib import Path
from typing import Self

import yaml
from citation_utils import Citation, ShortDocketCategory
from loguru import logger
from pydantic import BaseModel, Field
from sqlite_utils import Database

from corpus_sc_toolkit.meta import (
    CourtComposition,
    DecisionCategory,
    DecisionSource,
)
from corpus_sc_toolkit.resources import SC_LOCAL_FOLDER

from .interim_parts import InterimOpinion


class Fields(BaseModel):
    """A Decision will rely on the previous processing of various fields.

    This toolkit helps process some of those fields prior to insertion into a
    terminal database (even if they may previously originate from another
    third-party database.)

    Field | Type | Description
    :--:|:--:|:--
    created | float | When was this model instantiated, for paths, this is when the file was actually made
    modified | float |  When was this model last modified, for paths, this is when the file was actually modified
    id | str | The combination of various strings based on the source and citation, if available
    source | DecisionSource | May either be `sc` or `legacy`.
    origin | str | If `sc` source, this refers to the URL slug stem
    title | str | The case title
    description | str | The citation display
    date | datetime.date | The date the case was promulgated
    date_scraped | datetime.date | The date the case was scraped
    citation | optional[Citation] | The citation object
    composition | CourtComposition | Whether the court sat en banc or in division
    category | DecisionCategory | Whether the case decided was a decision or a resolution
    raw_ponente| optional[str] | Who decided the case, if available
    justice_id | optional[int] | The Justice ID, if available
    per_curiam | bool. Defaults to False. | Whether the case was decided per curiam
    is_pdf | bool. Defaults to False. | Whether the case originated from a PDF file
    fallo | optional[str] | Detected fallo / dispositive portion
    voting | optional[str] | Detected fallo / dispositive portion
    emails | list[str] | Emails of authors
    """  # noqa: E501

    created: float
    modified: float
    id: str
    source: DecisionSource = DecisionSource.sc
    origin: str
    title: str
    description: str
    date: datetime.date
    date_scraped: datetime.date
    citation: Citation | None = None
    composition: CourtComposition
    category: DecisionCategory
    raw_ponente: str | None = None
    justice_id: int | None = None
    per_curiam: bool = False
    is_pdf: bool = False
    fallo: str | None = None
    voting: str | None = None
    emails: list[str] = Field(default_factory=list)

    class Config:
        use_enum_values = True


class BaseDecision(Fields):
    """This can be formed by either loading data from a path or a
    previously created database.
    """

    @classmethod
    def from_path(cls, path: Path, db: Database) -> Self | None:
        from .from_path import decision_from_path

        return decision_from_path(path, db)


class InterimDecision(Fields):
    """Using the same BaseDecision instance, add its included
    Opinions (and each opinions's Segments.), and the means
    of populating the same via `cls.limited_decisions(<db>)`,
    where db refers to an sqlite database.

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
                    SELECT opinion_list
                    FROM opinion_list_data
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
            SELECT opinions
            FROM opinions_with_ponencia
        ) opinions
    FROM
        pre_tbl_decisions caso
        JOIN pre_tbl_decision_meta meta
        ON meta.decision_id = caso.id
    WHERE
        meta.notice = 0
    ```
    """

    opinions: list[InterimOpinion] = Field(default_factory=list)

    @classmethod
    def limited_decisions(cls, db: Database) -> Iterator[Self]:
        from .from_pdf import decision_from_pdf_db

        sql_path = Path(__file__).parent / "sql" / "limit_extract.sql"
        for row in db.execute_returning_dicts(sql_path.read_text()):
            if result := decision_from_pdf_db(db, row):
                yield result

    @property
    def is_dump_ok(self, target_path: Path = SC_LOCAL_FOLDER):
        if not target_path.exists():
            raise Exception("Cannot find target destination.")
        if not self.citation:
            logger.warning(f"No docket in {self.id=}")
            return False
        if self.citation.docket_category == ShortDocketCategory.BM:
            logger.warning(f"Manual check: BM docket in {self.id}.")
            return False
        return True

    def dump(self, target_path: Path = SC_LOCAL_FOLDER):
        if not self.is_dump_ok:
            return
        target_id = target_path / f"{self.id}"
        target_id.mkdir(exist_ok=True)
        with open(target_id / "_pdf.yml", "w+") as writefile:
            yaml.safe_dump(self.dict(), writefile)
            logger.debug(f"Built {target_id=}=")

    @classmethod
    def export(cls, db: Database, to_folder: Path = SC_LOCAL_FOLDER):
        for case in cls.limited_decisions(db):
            case.dump(to_folder)
