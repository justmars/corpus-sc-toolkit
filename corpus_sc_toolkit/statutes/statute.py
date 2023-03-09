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

from corpus_sc_toolkit.utils import sqlenv

from ._resources import Integrator, statute_storage


class StatuteRow(Page, StatuteBase, TableConfig):
    """This corresponds to `statute_trees.StatutePage` but is adjusted
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
    """This corresponds to `statute_patterns.StatuteTitle` but
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
        """Traverse tree and search unit caption /content for possible Statutes."""
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
        """Extract statute category and identifier pairs from the cls.__tablename__."""
        for row in c.db.execute_returning_dicts(
            sqlenv.get_template(
                "statutes/references/unique_statutes_list.sql"
            ).render(statute_references_tbl=cls.__tablename__)
        ):
            yield StatuteBase(**row).dict()

    @classmethod
    def update_statute_ids(cls, c: Connection) -> sqlite3.Cursor:
        """Since statutes present in `db`, supply `matching_statute_id` in
        references table."""
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
    prefix: str
    emails: list[EmailStr]
    meta: StatuteRow
    titles: list[StatuteTitleRow]
    tree: list[StatuteUnit]
    unit_fts: list[StatuteUnitSearch]
    material_paths: list[StatuteMaterialPath]
    statutes_found: list[StatuteFoundInUnit]

    @property
    def storage_meta(self) -> dict:
        return {
            "ID": self.id,
            "Prefix": self.prefix,
            "Title": self.meta.title,
            "Description": self.meta.description,
            "Category": self.meta.statute_category,
            "SerialId": self.meta.statute_serial_id,
            "Date": self.meta.date.isoformat(),
            "Variant": self.meta.variant,
        }

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

        # TODO: note that the id in page from StatutePage.build still uses dashes;
        # this needs to be fixed since the ID of statute will used the prefix_db_key
        if not page.prefix_db_key:
            logger.error(f"Could not make key {details_path=}")
            return None
        if not page.storage_prefix:
            logger.error(f"Could not make storage_prefix {details_path=}")
            return None

        # assign row for creation
        page.id = page.prefix_db_key  # mutate the page prior to export as dict
        meta = StatuteRow(**page.dict(exclude={"emails", "tree", "titles"}))
        statute_id = page.prefix_db_key  # use same id for related entities

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
            prefix=page.storage_prefix,
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

    def to_storage(self):
        loc = f"{self.prefix}/details.yaml"
        data = self.dict(exclude_none=True)
        temp_file = statute_storage.make_temp_yaml_path_from_data(data)
        args = statute_storage.set_extra_meta(self.storage_meta)
        statute_storage.upload(file_like=temp_file, loc=loc, args=args)
        temp_file.unlink()

    @classmethod
    def get(cls, prefix: str) -> Self:
        """Retrieve data represented by the `prefix` from R2 (implies previous
        `upload()`) and instantiate the Statute based on such retrieved data.

        Args:
            prefix (str): Must end with .yaml

        Returns:
            Self: Integrated Statute instance from R2 prefix.
        """
        if not (data := statute_storage.restore_temp_yaml(yaml_suffix=prefix)):
            raise Exception(f"Could not originate {prefix=}")
        return cls(**data)
