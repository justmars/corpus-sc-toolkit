from collections.abc import Iterator
from pathlib import Path
from typing import Self

from pydantic import BaseModel, Field
from sqlite_utils import Database

from corpus_sc_toolkit.justice import OpinionWriterName

SC_BASE_URL = "https://sc.judiciary.gov.ph"
PDF_DB_PATH: Path = Path().cwd() / "pdf.db"
TARGET_FOLDER: Path = Path().home() / "code" / "corpus" / "decisions" / "sc"
SQL_DECISIONS_ONLY = Path(__file__).parent / "sql" / "limit_extract.sql"


class ExtractSegmentPDF(BaseModel):
    id: str = Field(...)
    opinion_id: str = Field(...)
    decision_id: int = Field(...)
    position: str = Field(...)
    segment: str = Field(...)
    char_count: int = Field(...)


class ExtractOpinionPDF(BaseModel):
    id: str = Field(...)
    decision_id: int = Field(...)
    pdf: str
    writer: OpinionWriterName | None = Field(
        None,
        description=(
            "The writer of the opinion; when not supplied could mean a Per"
            " Curiam opinion, or unable to detect the proper justice."
        ),
    )
    justice_id: dict | None = Field(
        default_factory=dict,
        title="Justice ID",
        description=(  # noqa: E501
            "The result of matching the cleaned writer name with the database"
            " to get the id, if possible."
        ),
    )
    title: str | None = Field(
        ...,
        description=(
            "How is the opinion called, e.g. Ponencia, Concurring Opinion,"
            " Separate Opinion"
        ),
        col=str,
    )
    body: str = Field(..., description="Text proper of the opinion.")
    annex: str | None = Field(
        default=None, description="Annex portion of the opinion."
    )
    segments: list[ExtractSegmentPDF] = Field(default_factory=list)

    def get_segment(
        self,
        elements: list,
        opinion_id: str,
        text: str,
        position: str,
    ):
        if all(elements):
            return ExtractSegmentPDF(
                id="-".join(str(i) for i in elements),
                opinion_id=opinion_id,
                decision_id=self.decision_id,
                position=position,
                segment=text,
                char_count=len(text),
            )

    def _from_main(self, db: Database) -> Iterator[ExtractSegmentPDF]:
        """Populate segments from the main decision."""
        criteria = "decision_id = ? and length(text) > 10"
        params = (self.decision_id,)
        rows = db["pre_tbl_decision_segment"].rows_where(criteria, params)
        for row in rows:
            if segment := self.get_segment(
                elements=[row["id"], row["page_num"], self.decision_id],
                opinion_id=f"main-{self.decision_id}",
                text=row["text"],
                position=f"{row['id']}-{row['page_num']}",
            ):
                yield segment

    def _from_opinions(self, db: Database) -> Iterator[ExtractSegmentPDF]:
        """Populate segments from the opinion decision."""
        criteria = "opinion_id = ? and length(text) > 10"
        params = (self.id,)
        rows = db["pre_tbl_opinion_segment"].rows_where(criteria, params)
        for row in rows:
            if segment := self.get_segment(
                elements=[row["id"], row["page_num"], row["opinion_id"]],
                opinion_id=f"{str(self.decision_id)}-{row['opinion_id']}",
                text=row["text"],
                position=f"{row['id']}-{row['page_num']}",
            ):
                yield segment

    def with_segments_set(self, path: Path = PDF_DB_PATH) -> Self:
        db = Database(path)
        if self.title in ["Ponencia", "Notice"]:  # see limit_extract.sql
            self.segments = list(self._from_main(db))
        else:
            self.segments = list(self._from_opinions(db))
        return self
