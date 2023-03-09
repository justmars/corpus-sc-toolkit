import json
from collections.abc import Iterator
from concurrent.futures import TimeoutError
from typing import Self

from citation_utils import Citation
from dateutil.parser import parse
from loguru import logger
from pebble import concurrent
from pydantic import BaseModel, Field
from sqlite_utils import Database
from statute_trees import MentionedStatute

from corpus_sc_toolkit.utils import sqlenv

from ._resources import DECISION_BUCKET_NAME, DECISION_CLIENT, decision_storage
from .decision_fields import DecisionFields
from .decision_opinion_segments import OpinionSegment
from .decision_opinions import DecisionOpinion, OpinionTag
from .fields import CourtComposition, DecisionCategory
from .justice import CandidateJustice

PDF_KEY = "pdf.yaml"
"""This suffix is uploaded / retrieved from R2 storage based on `DecisionPDF`."""


@concurrent.process(timeout=5)
def list_statutes(body_text: str, annex_text: str | None = None):
    items = []
    items.extend(list(MentionedStatute.set_counted_statute(text=body_text)))
    if annex_text:
        items.extend(
            list(MentionedStatute.set_counted_statute(text=annex_text))
        )
    return items


@concurrent.process(timeout=5)
def list_citations(body_text: str, annex_text: str | None = None):
    items = []
    items.extend(list(Citation.extract_citations(text=body_text)))
    if annex_text:
        items.extend(list(Citation.extract_citations(text=annex_text)))
    return items


def extract_citation_ids(data: dict) -> tuple[str, str, Citation] | None:
    """Because of inconsistent data declaration in the PDF table,
    need a separate citation extractor. This oresumes existence of
    the following keys:

    1. docket_category
    2. serial
    3. date
    """
    keys = ["docket_category", "serial", "date"]
    if not all([data.get(k) for k in keys]):
        return None
    date_obj = parse(data["date"]).date()
    docket_partial = f"{data['docket_category']} No. {data['serial']}"
    docket_str = f"{docket_partial}, {date_obj.strftime('%b %-d, %Y')}"
    cite = Citation.extract_citation(docket_str)
    if not cite:
        return None
    decision_id = cite.prefix_db_key
    if not decision_id:
        return None
    decision_prefix = cite.storage_prefix
    if not decision_prefix:
        return None
    return decision_id, decision_prefix, cite


class InterimOpinion(BaseModel):
    id: str = Field(default=...)
    decision_id: str = Field(default=...)
    candidate: CandidateJustice = Field(exclude=True)
    title: str = Field(
        default=...,
        description=(
            "How is the opinion called, e.g. Ponencia, Concurring Opinion,"
            " Separate Opinion"
        ),
        col=str,
    )
    body: str = Field(
        default=...,
        title="Opinion Body",
        description="Text proper of the opinion.",
    )
    annex: str | None = Field(
        default=None,
        title="Opinion Annex",
        description="Annex portion of the opinion.",
    )
    pdf: str = Field(description="Downloadable link to the opinion pdf.")

    class Config:
        arbitrary_types_allowed = True

    @property
    def row(self):
        """Row to be used in OpinionRow table."""
        id = f"{self.decision_id}-{self.candidate.id or self.title.lower()}"
        text = f"{self.body}\n\n----\n\n{self.annex}"

        citations = []
        future_citations = list_citations(self.body, self.annex)  # type: ignore
        try:
            citations = future_citations.result()  # type: ignore
        except TimeoutError:
            logger.error(f"Timed out citations {self.id}")
        except Exception:
            logger.error(f"Could not generate citations {self.id}")

        statutes = []
        future_statutes = list_statutes(self.body, self.annex)  # type: ignore
        try:
            statutes = future_statutes.result()  # type: ignore
        except TimeoutError:
            logger.error(f"Timed out statutes {self.id}")
        except Exception:
            logger.error(f"Could not generate statutes {self.id}")

        return DecisionOpinion(
            id=id,
            decision_id=self.decision_id,
            title=self.title,
            pdf=self.pdf,
            justice_id=self.candidate.id,
            text=text,
            tags=OpinionTag.detect(self.title),
            citations=citations,
            statutes=statutes,
            segments=list(
                OpinionSegment.make_segments(
                    decision_id=self.decision_id,
                    opinion_id=id,
                    text=text,
                )
            ),
        )

    @classmethod
    def setup(cls, idx: str, db: Database, data: dict) -> dict | None:
        """This will partially process the sql query defined in
        `/sql/limit_extract.sql` The required fields in `data`:

        1. `opinions` - i.e. a string made of `json_group_array`, `json_object` from sqlite query
        2. `date` - for determining the justice involved in the opinion/s
        """  # noqa: E501
        opinions = []
        match_ponencia = {}
        prerequisite = "id" in data and "date" in data and "opinions" in data
        if not prerequisite:
            return None
        subkeys = ["id", "title", "body", "annex"]
        for op in json.loads(data["opinions"]):
            pdf = f"https://sc.judiciary.gov.ph{op['pdf']}"
            raw = {k: v for k, v in op.items() if k in subkeys}
            candidate = CandidateJustice(db, op.get("writer"), data["date"])
            opinion = cls(decision_id=idx, pdf=pdf, candidate=candidate, **raw)
            if opinion.title == "Ponencia":
                if opinion.candidate and opinion.candidate.detail:
                    match_ponencia = opinion.candidate.detail._asdict()
            opinions.append(opinion)
        return {"opinions": opinions} | match_ponencia


class DecisionPDF(DecisionFields):
    ...

    @classmethod
    def get_key(cls, dated_prefix: str) -> str | None:
        """Is suffix `pdf.yaml` present in result of `cls.iter_dockets()`?"""
        key = f"{dated_prefix}/{PDF_KEY}"
        try:
            DECISION_CLIENT.get_object(Bucket=DECISION_BUCKET_NAME, Key=key)
            return key
        except Exception:
            return None

    @classmethod
    def get_existing_prefixes(cls) -> Iterator[str]:
        if object_list := decision_storage.all_items():
            if filtered_list := decision_storage.filter_content(
                filter_suffix="/{PDF_KEY}", objects_list=object_list
            ):
                for item in filtered_list:
                    yield item["Key"]

    def to_storage(self):
        # Uses `pdf.yaml` to upload decision fields represented by instance.
        logger.debug(f"Uploading: {self.id=}")
        self.put_in_storage(PDF_KEY)

        # Upload txt-based opinion files
        for opinion in self.opinions:
            opinion.to_storage(self.prefix, "txt")

    @classmethod
    def originate(cls, db: Database) -> Iterator[Self]:
        """Extract sql query (`/sql/limit_extract.sql`) from `db` to instantiate
        a list of rows to process.

        Args:
            db (Database): result of a [corpus-extractor](https://github.com/justmars/corpus-extractor/) process.

        Yields:
            Iterator[Self]: Instances of the Interim Decision.
        """  # noqa: E501
        q = sqlenv.get_template("decisions/limit_extract.sql").render()
        for row in db.execute_returning_dicts(q):
            result = extract_citation_ids(row)
            if not result:
                logger.error(
                    f"No citation from {row.get('docket_category')=} {row.get('serial')=} {row.get('date')=}"  # noqa: E501
                )
                continue
            decision_id, decision_prefix, cite = result
            decision = cls(
                id=decision_id,
                prefix=decision_prefix,
                is_pdf=True,
                origin=row["id"],
                title=row["title"],
                description=cite.display,
                date=parse(row["date"]).date(),
                date_scraped=parse(row["scraped"]).date(),
                citation=cite,
                composition=CourtComposition._setter(text=row["composition"]),
                emails=["bot@lawsql.com"],
                category=DecisionCategory.set_category(
                    row.get("category"), row.get("notice")
                ),
            )
            opx_data = InterimOpinion.setup(idx=decision_id, db=db, data=row)
            if not opx_data or not opx_data.get("opinions"):
                logger.error(f"No opinions detected in {decision_id=}")
                continue

            decision.raw_ponente = opx_data.get("raw_ponente", None)
            decision.per_curiam = opx_data.get("per_curiam", False)
            decision.justice_id = opx_data.get("justice_id", None)
            decision.opinions = [op.row for op in opx_data["opinions"]]
            yield decision
