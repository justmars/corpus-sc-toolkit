import datetime
from typing import Any
import json
import yaml
from collections.abc import Iterator
from pathlib import Path
from typing import Self

from dateutil.parser import parse

from citation_utils import ShortDocketCategory
from loguru import logger
from pydantic import Field, BaseModel
from sqlite_utils import Database

from corpus_sc_toolkit.resources import SC_LOCAL_FOLDER, SC_BASE_URL
from corpus_sc_toolkit.meta import (
    CourtComposition,
    DecisionCategory,
    DecisionSource,
    get_cite_from_fields,
    get_id_from_citation,
)
from corpus_sc_toolkit.justice import CandidateJustice
from .fields import OpinionSegment, DecisionFields


class InterimOpinion(BaseModel):
    id: str = Field(...)
    decision_id: str = Field(...)
    candidate: CandidateJustice
    title: str | None = Field(
        ...,
        description=(
            "How is the opinion called, e.g. Ponencia, Concurring Opinion,"
            " Separate Opinion"
        ),
        col=str,
    )
    body: str = Field(
        ...,
        title="Opinion Body",
        description="Text proper of the opinion.",
    )
    annex: str | None = Field(
        default=None,
        title="Opinion Annex",
        description="Annex portion of the opinion.",
    )
    segments: list[OpinionSegment] = Field(
        default_factory=list,
        title="Opinion Segments",
        description="Each body segment of the Opinion Body.",
    )
    pdf: str = Field(description="Downloadable link to the opinion pdf.")

    class Config:
        arbitrary_types_allowed = True

    @property
    def row(self) -> dict[str, str]:
        """Row to be used in OpinionRow table."""
        text = f"{self.body}\n----\n{self.annex}"
        base = self.dict(include={"id", "decision_id", "pdf", "title"})
        extended = {"justice_id": self.candidate.id, "text": text}
        return base | extended

    @property
    def ponencia_meta(self):
        """Used to return relevant details of the ponencia in `setup()`"""
        if self.title == "Ponencia":
            if self.candidate and self.candidate.detail:
                return self.candidate.detail._asdict()
        return None

    @classmethod
    def setup(cls, idx: str, db: Database, data: dict) -> dict | None:
        """Presumes existence of the following keys:

        This will partially process the sql query defined in
        `/sql/limit_extract.sql`

        The required fields in `data`:

        1. `opinions` - i.e. a string made of `json_group_array`, `json_object` from sqlite query
        2. `date` - for determining the justice involved in the opinion/s
        """  # noqa: E501
        prerequisite = "id" in data and "date" in data and "opinions" in data
        if not prerequisite:
            return None

        opinions = []
        match_ponencia = {}
        keys = ["id", "title", "body", "annex"]
        for op in json.loads(data["opinions"]):
            opinion = cls(
                decision_id=idx,
                pdf=f"{SC_BASE_URL}{op['pdf']}",
                candidate=CandidateJustice(db, op.get("writer"), data["date"]),
                **{k: v for k, v in op.items() if k in keys},
            )
            opinion.add_segments(db=db, id=data["id"])
            opinions.append(opinion)
            if opinion.ponencia_meta:
                match_ponencia = opinion.ponencia_meta
        return {"opinions": opinions} | match_ponencia

    def add_segments(self, db: Database, id: int):
        if self.title in ["Ponencia", "Notice"]:  # see limit_extract.sql
            tbl = db["pre_tbl_decision_segment"]
            criteria = "decision_id = ? and length(text) > 10"
            params = (id,)  # refers to the **unaltered** decision id
            rows = tbl.rows_where(where=criteria, where_args=params)
            for row in rows:
                if segment := OpinionSegment.set(
                    elements=[row["id"], row["page_num"], self.decision_id],
                    opinion_id=f"main-{self.decision_id}",
                    decision_id=self.decision_id,
                    text=row["text"],
                    position=f"{row['id']}-{row['page_num']}",
                ):
                    self.segments.append(segment)
        else:
            tbl = db["pre_tbl_opinion_segment"]
            criteria = "opinion_id = ? and length(text) > 10"
            params = (self.id,)  # refers to the opinion id
            rows = tbl.rows_where(where=criteria, where_args=params)
            for row in rows:
                if segment := OpinionSegment.set(
                    elements=[row["id"], row["page_num"], row["opinion_id"]],
                    opinion_id=f"{str(self.decision_id)}-{row['opinion_id']}",
                    decision_id=self.decision_id,
                    text=row["text"],
                    position=f"{row['id']}-{row['page_num']}",
                ):
                    self.segments.append(segment)


def decision_from_pdf_db(db: Database, row: dict[str, Any]) -> dict | None:
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

    idx = get_id_from_citation(
        folder_name=row["id"], source=DecisionSource.sc.value, citation=cite
    )
    opx = InterimOpinion.setup(idx=idx, db=db, data=row)
    if not opx or not opx.get("opinions"):
        logger.error(f"No opinions detected in {row['id']=}")
        return None

    return dict(
        id=idx,
        origin=row["id"],
        title=row["title"],
        description=cite.display,
        created=datetime.datetime.now().timestamp(),
        modified=datetime.datetime.now().timestamp(),
        date=parse(row["date"]).date(),
        date_scraped=parse(row["scraped"]).date(),
        citation=cite,
        composition=CourtComposition._setter(text=row["composition"]),
        category=DecisionCategory.set_category(
            category=row.get("category"),
            notice=row.get("notice"),
        ),
        opinions=opx["opinions"],
        raw_ponente=opx.get("raw_ponente", None),
        per_curiam=opx.get("per_curiam", False),
        justice_id=opx.get("justice_id", None),
        is_pdf=True,
        emails=["bot@lawsql.com"],
    )


class InterimDecision(DecisionFields):
    """An Interim Decision is a container for fields
    with a function to load these fields from a given database. The
    The pre-existing database is required to source both the `justice_id`
    and the fields previously retrieved from pdf files in corpus-extractor.
    """

    opinions: list[InterimOpinion] = Field(default_factory=list)

    @classmethod
    def _get(cls, db: Database) -> Iterator[Self]:
        """Given a database, extract opinion-level content for each
        decision via this sql query:

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

        Args:
            db (Database): _description_

        Yields:
            Iterator[Self]: _description_
        """
        sql_path = Path(__file__).parent / "sql" / "limit_extract.sql"
        for row in db.execute_returning_dicts(sql_path.read_text()):
            if result := decision_from_pdf_db(db, row):
                yield cls(**result)

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
        for case in cls._get(db):
            case.dump(to_folder)
