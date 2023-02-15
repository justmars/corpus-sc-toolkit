import json
from collections.abc import Iterator
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

    @classmethod
    def setup(cls, idx: str, db: Database, data: dict) -> dict | None:
        """Presumes existence of the following keys:

        This will partially process the sql query defined in
        `/sql/limit_extract.sql`

        The required fields in `data`:

        1. `opinions` - i.e. a string made of `json_group_array`, `json_object` from sqlite query
        2. `date` - for determining the justice involved in the opinion/s
        """  # noqa: E501
        opinions = []
        match_ponencia = None
        fields_present = "date" in data and "opinions" in data
        if not fields_present:
            return None
        for op in json.loads(data["opinions"]):
            opinion = cls(
                id=op["id"],
                decision_id=idx,
                pdf=f"{SC_BASE_URL}{op['pdf']}",
                candidate=CandidateJustice(db, op.get("writer"), data["date"]),
                title=op["title"],
                body=op["body"],
                annex=op["annex"],
            ).with_segments_set(db=db)
            opinions.append(opinion)
            if not match_ponencia and opinion.title == "Ponencia":
                match_ponencia = opinion.candidate
        return {"opinions": opinions} | (
            match_ponencia.detail._asdict()
            if match_ponencia and match_ponencia.detail
            else {}
        )

    def _from_main(self, db: Database) -> Iterator[InterimSegment]:
        """Populate segments from the main decision."""
        for row in db["pre_tbl_decision_segment"].rows_where(
            where="decision_id = ? and length(text) > 10",
            where_args=(self.decision_id,),
        ):
            if segment := InterimSegment.set(
                elements=[row["id"], row["page_num"], self.decision_id],
                opinion_id=f"main-{self.decision_id}",
                decision_id=self.decision_id,
                text=row["text"],
                position=f"{row['id']}-{row['page_num']}",
            ):
                yield segment

    def _from_opinions(self, db: Database) -> Iterator[InterimSegment]:
        """Populate segments from the opinion decision."""
        for row in db["pre_tbl_opinion_segment"].rows_where(
            where="opinion_id = ? and length(text) > 10",
            where_args=(self.id,),
        ):
            if segment := InterimSegment.set(
                elements=[row["id"], row["page_num"], row["opinion_id"]],
                opinion_id=f"{str(self.decision_id)}-{row['opinion_id']}",
                decision_id=self.decision_id,
                text=row["text"],
                position=f"{row['id']}-{row['page_num']}",
            ):
                yield segment

    def with_segments_set(self, db: Database) -> Self:
        if self.title in ["Ponencia", "Notice"]:  # see limit_extract.sql
            self.segments = list(self._from_main(db))
        else:
            self.segments = list(self._from_opinions(db))
        return self
