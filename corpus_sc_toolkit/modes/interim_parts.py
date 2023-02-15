import json
from typing import NamedTuple, Self

from pydantic import BaseModel, Field
from sqlite_utils import Database

from corpus_sc_toolkit.justice import CandidateJustice
from corpus_sc_toolkit.resources import SC_BASE_URL


class InterimSegment(NamedTuple):
    id: str
    opinion_id: str
    decision_id: str
    position: str
    segment: str
    char_count: int

    @classmethod
    def set(
        cls,
        elements: list,
        opinion_id: str,
        text: str,
        position: str,
        decision_id: str,
    ):
        if all(elements):
            return cls(
                id="-".join(str(i) for i in elements),
                opinion_id=opinion_id,
                decision_id=decision_id,
                position=position,
                segment=text,
                char_count=len(text),
            )


class InterimOpinion(BaseModel):
    id: str = Field(...)
    decision_id: str = Field(...)
    pdf: str = Field(description="Downloadable link to the opinion pdf.")
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
    segments: list[InterimSegment] = Field(
        default_factory=list,
        title="Opinion Segments",
        description="Each body segment of the Opinion Body.",
    )

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
                if segment := InterimSegment.set(
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
                if segment := InterimSegment.set(
                    elements=[row["id"], row["page_num"], row["opinion_id"]],
                    opinion_id=f"{str(self.decision_id)}-{row['opinion_id']}",
                    decision_id=self.decision_id,
                    text=row["text"],
                    position=f"{row['id']}-{row['page_num']}",
                ):
                    self.segments.append(segment)
