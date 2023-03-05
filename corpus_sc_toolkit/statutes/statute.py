import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Self

from loguru import logger
from pydantic import EmailStr, Field, ValidationError
from sqlpyd import Connection, TableConfig
from statute_patterns import (
    StatuteTitleCategory,
    extract_rules,
)
from statute_trees import (
    Node,
    Page,
    StatuteBase,
    StatutePage,
    StatuteUnit,
    generic_content,
    generic_mp,
)

from .._utils import (
    ascii_singleline,
    create_temp_yaml,
    download_to_temp,
    sqlenv,
)
from ._resources import STATUTE_DETAILS_SUFFIX, STATUTE_ORIGIN, Integrator


class StatuteRow(Page, StatuteBase, TableConfig):
    """This corresponds to statute_trees.StatutePage but is adjusted
    for the purpose of table creation."""

    __prefix__ = "lex"
    __tablename__ = "statutes"
    __indexes__ = [
        ["statute_category", "statute_serial_id", "date", "variant"],
        ["statute_category", "statute_serial_id", "date"],
        ["statute_category", "statute_serial_id", "variant"],
        ["statute_category", "statute_serial_id"],
    ]

    @classmethod
    def get_id_via_catid(cls, c: Connection, cat: str, id: str) -> str | None:
        tbl = c.table(cls)
        q = "statute_category = ? and statute_serial_id = ?"
        rows = list(tbl.rows_where(where=q, where_args=(cat, id), select="id"))
        idx = rows[0]["id"] if rows else None
        return idx

    @classmethod
    def get_id(cls, c: Connection, pk: str) -> str | None:
        tbl = c.table(cls)
        q = "id = ?"
        rows = list(tbl.rows_where(where=q, where_args=(pk,), select="id"))
        idx = rows[0]["id"] if rows else None
        return idx


class StatuteTitleRow(TableConfig):
    """This corresponds to statute_patterns.StatuteTitle but
    is adjusted for the purpose of table creation."""

    __prefix__ = "lex"
    __tablename__ = "statute_titles"
    __indexes__ = [["category", "text"], ["category", "statute_id"]]
    statute_id: str = Field(..., col=str, fk=(StatuteRow.__tablename__, "id"))
    category: StatuteTitleCategory = Field(
        ...,
        col=str,
        index=True,
    )
    text: str = Field(..., col=str, fts=True)

    class Config:
        use_enum_values = True


class StatuteUnitSearch(TableConfig):
    __prefix__ = "lex"
    __tablename__ = "statute_fts_units"
    __indexes__ = [["statute_id", "material_path"]]
    statute_id: str = Field(..., col=str, fk=(StatuteRow.__tablename__, "id"))
    material_path: str = generic_mp
    unit_text: str = generic_content


class StatuteMaterialPath(Node, TableConfig):
    __prefix__ = "lex"
    __tablename__ = "statute_mp_units"
    __indexes__ = [
        ["item", "caption", "content", "statute_id"],
        ["item", "caption", "statute_id"],
        ["item", "content", "statute_id"],
        ["item", "statute_id"],
    ]
    statute_id: str = Field(..., col=str, fk=(StatuteRow.__tablename__, "id"))
    material_path: str = generic_mp


class StatuteFoundInUnit(StatuteBase, TableConfig):
    """Each unit in Statute A (see MP) may refer to Statute B.
    Statute B is referenced through it's category and identifier
    (hence inheriting from `StatuteBase`). After securing the category
    and identifier pairs, can use the `cls.update_statute_ids()` to
    supply the matching statute  id of the category/identifier pair.
    """

    __prefix__ = "lex"
    __tablename__ = "statute_unit_references"
    __indexes__ = [
        ["statute_category", "statute_serial_id"],
        ["statute_category", "statute_id"],
    ]
    statute_id: str = Field(..., col=str, fk=(StatuteRow.__tablename__, "id"))
    material_path: str = generic_mp
    matching_statute_id: str | None = Field(
        None,
        description=(
            "Each unit in Statute A (see MP) may refer to Statute B."
            " Statute B is referenced through it's category and identifier"
            " (see StatuteBase)."
        ),
        col=str,
        fk=(StatuteRow.__tablename__, "id"),
    )

    @classmethod
    def list_affected_statutes(cls, c: Connection, pk: str) -> dict:
        sql_file = "statutes/list_affected_statutes.sql"
        results = c.db.execute_returning_dicts(
            sqlenv.get_template(sql_file).render(
                ref_tbl=cls.__tablename__,
                statute_tbl=StatuteRow.__tablename__,
                affecting_statute_id=pk,
            )
        )
        if results:
            return results[0]
        return {}

    @classmethod
    def list_affector_statutes(cls, c: Connection, pk: str) -> dict:
        sql_file = "statutes/list_affector_statutes.sql"
        results = c.db.execute_returning_dicts(
            sqlenv.get_template(sql_file).render(
                ref_tbl=cls.__tablename__,
                statute_tbl=StatuteRow.__tablename__,
                affected_statute_id=pk,
            )
        )
        if results:
            return results[0]
        return {}

    @classmethod
    def find_statute_in_unit(
        cls,
        text: str,
        mp: str,
        statute_id: str,
    ) -> Iterator["StatuteFoundInUnit"]:
        """Given text of a particular `material_path`, determine if there are
        statutes found by `get_statute_labels`; if they're found, determine
        the proper `StatuteFoundInUnit` to yield.
        """
        for rule in extract_rules(text):
            yield cls(
                material_path=mp,
                statute_id=statute_id,
                statute_category=rule.cat,
                statute_serial_id=rule.id,
                matching_statute_id=None,
            )

    @classmethod
    def extract_units(
        cls,
        pk: str,
        units: list["StatuteUnit"],
    ) -> Iterator["StatuteFoundInUnit"]:
        """Traverse the tree and search the caption and content of each unit
        for possible Statutes.
        """
        for u in units:
            if u.caption and u.content:
                text = f"{u.caption}. {u.content}"
                yield from cls.find_statute_in_unit(text, u.id, pk)
            elif u.content:
                yield from cls.find_statute_in_unit(u.content, u.id, pk)
            if u.units:
                yield from cls.extract_units(pk, u.units)

    @classmethod
    def get_statutes_from_references(cls, c: Connection) -> Iterator[dict]:
        """Extract relevant statute category and identifier pairs
        from the cls.__tablename__."""
        for row in c.db.execute_returning_dicts(
            sqlenv.get_template(
                "statutes/references/unique_statutes_list.sql"
            ).render(statute_references_tbl=cls.__tablename__)
        ):
            yield StatuteBase(**row).dict()

    @classmethod
    def update_statute_ids(cls, c: Connection) -> sqlite3.Cursor:
        """Since all statutes present in `db`, supply `matching_statute_id` in
        the references table."""
        with c.session as cur:
            return cur.execute(
                sqlenv.get_template("statutes/update_id.sql").render(
                    statute_tbl=StatuteRow.__tablename__,
                    target_tbl=cls.__tablename__,
                    target_col=cls.__fields__["matching_statute_id"].name,
                )
            )


class Statute(Integrator):
    id: str
    emails: list[EmailStr]
    meta: StatuteRow
    titles: list[StatuteTitleRow]
    tree: list[StatuteUnit]
    unit_fts: list[StatuteUnitSearch]
    material_paths: list[StatuteMaterialPath]
    statutes_found: list[StatuteFoundInUnit]

    @property
    def relations(self):
        return [
            (StatuteMaterialPath, self.material_paths),
            (StatuteUnitSearch, self.unit_fts),
            (StatuteTitleRow, self.titles),
            (StatuteFoundInUnit, self.statutes_found),
        ]

    @classmethod
    def from_page(cls, details_path: Path) -> Self | None:
        """Assumes a local directory from which to construct statutory objects.

        Args:
            details_path (Path): See `StatutePage` for original fields required in
                the necessary yaml path.

        Returns:
            Self | None: If all fields validate, return an instance of an Integrated
                Statute.
        """
        # build and validate metadata from the path
        try:
            page = StatutePage.build(details_path)
        except ValidationError:
            logger.error(f"Could not validate {details_path=}")
            return None

        # assign row for creation
        meta = StatuteRow(**page.dict(exclude={"emails", "tree", "titles"}))

        # use identifiers to create unique id
        base_prefix = cls.get_base_prefix(meta)
        if not base_prefix:
            return None
        statute_id = cls.set_prefix_id(base_prefix)

        # setup associated titles
        titles = [
            StatuteTitleRow(**statute_title.dict())
            for statute_title in page.titles
        ]

        # enable full text searches of contents of the tree; starts with `1.1.`
        fts = [
            StatuteUnitSearch(**unit)
            for unit in StatuteUnit.searchables(statute_id, page.tree)
        ]

        # full text searches should includes a title row, i.e. node `1.``
        root_fts = [
            StatuteUnitSearch(
                statute_id=statute_id,
                material_path="1.",
                unit_text=", ".join(
                    [f"{meta.statute_category} {meta.statute_serial_id}"]
                    + [t.text for t in titles]
                ),
            )
        ]

        return Statute(
            id=statute_id,
            emails=page.emails,
            meta=meta,
            tree=page.tree,
            titles=titles,
            unit_fts=root_fts + fts,
            material_paths=[
                StatuteMaterialPath(**unit)
                for unit in StatuteUnit.granularize(
                    pk=statute_id, nodes=page.tree
                )
            ],
            statutes_found=list(
                StatuteFoundInUnit.extract_units(
                    pk=statute_id, units=page.tree
                )
            ),
        )

    @classmethod
    def make_tables(cls, c: Connection):
        """The bulk of the fields declared within the Statute
        container are table structures."""
        c.create_table(StatuteRow)  # corresponds to StatutePage
        c.create_table(StatuteTitleRow)  # corresponds to StatuteTitle
        c.create_table(StatuteUnitSearch)
        c.create_table(StatuteMaterialPath)
        c.create_table(StatuteFoundInUnit)
        c.db.index_foreign_keys()

    @classmethod
    def get_base_prefix(cls, statute_meta_obj: StatuteRow) -> str | None:
        """If the model were to be stored in cloud storage like R2,
        this property ensures a unique prefix for the instance. Should
        be in the following format: `<statute_category>/<statute_serial_id>/<variant>/`,
        e.g. `ra/386/1`
        """
        if not statute_meta_obj.statute_category:
            return None
        if not statute_meta_obj.statute_serial_id:
            return None
        if not statute_meta_obj.variant:
            return None
        return "/".join(
            str(i)
            for i in [
                statute_meta_obj.statute_category,
                statute_meta_obj.statute_serial_id,
                statute_meta_obj.variant,
            ]
        )

    @classmethod
    def set_prefix_id(cls, text: str):
        return text.replace("/", "-").lower()

    @property
    def base_prefix(self):
        return self.id.replace("-", "/").lower()

    @property
    def storage_meta(self):
        """When uploading to R2, the metadata can be included as extra arguments to
        the file."""
        if not any(
            [
                self.meta.statute_category,
                self.meta.statute_serial_id,
                self.meta.date,
            ]
        ):
            return {}
        raw = {
            "Statute_Title": self.meta.title,
            "Statute_Description": ascii_singleline(self.meta.description),
            "Statute_Category": self.meta.statute_category,
            "Statute_Serial_Id": self.meta.statute_serial_id,
            "Statute_Date": self.meta.date.isoformat(),
            "Statute_Variant": self.meta.variant,
        }
        return {"Metadata": {k: str(v) for k, v in raw.items() if v}}

    def upload(self):
        STATUTE_ORIGIN.upload(
            file_like=create_temp_yaml(self.dict()),
            loc=f"{self.base_prefix}/{STATUTE_DETAILS_SUFFIX}",
            args=self.storage_meta,
        )

    @classmethod
    def get(cls, prefix: str) -> Self:
        """Retrieve data represented by the `prefix` from R2 (implies previous
        `upload()`) and instantiate the Statute based on such retrieved data.

        Args:
            prefix (str): Must end with /DETAILS.yaml

        Returns:
            Self: Interim Decision instance from R2 prefix.
        """
        if not prefix.endswith(STATUTE_DETAILS_SUFFIX):
            raise Exception("Method limited to details-based files.")
        data = download_to_temp(bucket=STATUTE_ORIGIN, src=prefix, ext="yaml")
        if not isinstance(data, dict):
            raise Exception(f"Could not originate {prefix=}")
        return cls(**data)
