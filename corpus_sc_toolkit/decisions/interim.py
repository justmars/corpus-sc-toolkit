import json
from collections.abc import Iterator
from pathlib import Path
from typing import Self

from citation_utils import Citation
from dateutil.parser import parse
from loguru import logger
from pydantic import BaseModel, Field
from sqlite_utils import Database

from .._utils import sqlenv
from ._resources import SUFFIX_PDF, decision_storage
from .decision_fields import DecisionFields
from .decision_substructures import DecisionOpinion
from .fields import CourtComposition, DecisionCategory
from .justice import CandidateJustice


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
        return DecisionOpinion(
            id=f"{self.decision_id}-{self.candidate.id or self.id}",
            decision_id=self.decision_id,
            title=self.title,
            pdf=self.pdf,
            justice_id=self.candidate.id,
            text=f"{self.body}\n\n----\n\n{self.annex}",
            tags=[],
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


class InterimDecision(DecisionFields):
    ...

    @classmethod
    def originate(cls, db: Database) -> Iterator[Self]:
        """Extract sql query (`/sql/limit_extract.sql`) from `db` to instantiate
        a list of rows to process.

        Args:
            db (Database): Contains previously created pdf-based / justice tables.

        Yields:
            Iterator[Self]: Instances of the Interim Decision.
        """
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

    @property
    def pdf_prefix(self) -> str | None:
        """Represents the pdf prefix to be uploaded to R2."""
        if not self.is_pdf:
            logger.warning("Method limited to pdf-based files.")
            return None
        return f"{self.prefix}{SUFFIX_PDF}"

    def dump(self) -> tuple[str, Path] | None:
        """Create a temporary yaml file containing the relevant fields
        of the Interim Decision instance and pair this file with its
        intended target prefix when it gets uploaded to storage. This is
        the resulting tuple.

        The prefix implies that a docket citation exists since the pdf
        data will be uploaded to a `<prefix>/pdf.yaml` endpoint.

        Returns:
            tuple[str, Path] | None: prefix and Path, if the prefix exists.
        """
        if not self.pdf_prefix:
            return None
        source = self.dict(exclude={"opinions"})
        data = source | {"opinions": [o.dict() for o in self.opinions]}
        temp_path = decision_storage.make_temp_yaml_path_from_data(data)
        return self.pdf_prefix, temp_path

    def upload(self):
        """With a temporary file prepared, upload the object representing
        the Interim Decision instance to R2 storage.

        Args:
            override (bool, optional): If true, will override. Defaults to False.

        Returns:
            bool: Whether or not an upload was performed.
        """
        prep_pdf_upload = self.dump()
        if not prep_pdf_upload:
            return False
        loc, file_like = prep_pdf_upload
        logger.info(f"Uploading {loc=}")
        decision_storage.upload(
            file_like=file_like, loc=loc, args=self.storage_meta
        )

    @classmethod
    def get(cls, prefix: str) -> Self:
        """Retrieve data represented by the `prefix` from R2 (implies previous
        from `dump()` and `upload()`) and instantiate the Interim Decision based
        on such retrieved data.

        Args:
            prefix (str): Must end with /pdf.yaml

        Returns:
            Self: Interim Decision instance from R2 prefix.
        """
        data = decision_storage.restore_temp_yaml(prefix)
        if not data:
            raise Exception(f"Could not originate {prefix=}")
        opinions = [DecisionOpinion(**o) for o in data.pop("opinions")]
        return cls(opinions=opinions, **data)
