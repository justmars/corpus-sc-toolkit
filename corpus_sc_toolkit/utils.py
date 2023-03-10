import re
from dataclasses import dataclass
from pathlib import Path

from corpus_pax import Individual
from jinja2 import Environment, PackageLoader, select_autoescape
from markdownify import markdownify
from sqlpyd import Connection

sqlenv = Environment(
    loader=PackageLoader(
        package_name="corpus_sc_toolkit", package_path="_sql"
    ),
    autoescape=select_autoescape(),
)


def sql_get_detail(generic_tbl_name: str, generic_id: str) -> str:
    return sqlenv.get_template("base/get_detail.sql").render(
        generic_tbl=generic_tbl_name,
        target_id=generic_id,
    )


def sql_get_authors(generic_tbl_name: str, generic_id: str) -> str:
    """Produce the SQL query string necessary to get the authors from the
    Individual table based on the `generic_tbl_name`'s target `generic_id`.

    Each generic_tbl_name will be sourced from either: DecisionRow,
    CodeRow, DocRow, StatuteRow. Each of these tables are associated
    with the Individual table. The result looks something like this:

    Examples:
        >>> from .statutes import StatuteRow
        >>> sql = sql_get_authors(StatuteRow.__tablename__, "ra-386-june-18-1949")
        >>> type(sql)
        <class 'str'>


    See sqlite_utils which creates m2m object tables after sorting the tables
    alphabetically.
    """
    tables = [generic_tbl_name, Individual.__tablename__]
    template = sqlenv.get_template("base/get_author_ids.sql")
    return template.render(
        generic_tbl="_".join(sorted(tables)),
        col_generic_obj="_".join([generic_tbl_name, "id"]),
        col_author_id="_".join([Individual.__tablename__, "id"]),
        target_id=generic_id,
    )


def get_authored_object(
    c: Connection, generic_tbl_name: str, generic_id: str
) -> dict:
    tbl = generic_tbl_name
    idx = generic_id
    a = c.db.execute_returning_dicts(sql_get_detail(tbl, idx))[0]
    b = c.db.execute_returning_dicts(sql_get_authors(tbl, idx))[0]
    result = a | b
    return result


"""Common Jinja2-based environment to get templates from a common path."""

MD_FOOTNOTE_LEFT = re.compile(
    r"""
    \^\[ # start with ^[ enclosing digits ending in ], targets the "^["
    (?=\d+\])
    """,
    re.X,
)

MD_FOOTNOTE_RIGHT_POST_LEFT = re.compile(
    r"""
    (?<=\^\d)(\]\^)| # single digit between ^ and ]^ targets the "]^"
    (?<=\^\d{2})(\]\^)| # double digit between ^ and ]^ targets the "]^"
    (?<=\^\d{3})(\]\^) # triple digit between ^ and ]^ targets the "]^"
    """,
    re.X,
)

MD_FOOTNOTE_AFTER_LEFT_RIGHT = re.compile(
    r"""
    (?<=\^\d\])\s| # single digit between ^[ and ], targets the space \s after
    (?<=\^\d{2}\])\s| # double digit between ^[ and ], targets the space \s after
    (?<=\^\d{3}\])\s # triple digit between ^[ and ], targets the space \s after
    """,
    re.X,
)


@dataclass
class DecisionHTMLConvertMarkdown:
    """Presumes the existence of various html files to construct a markdown document.

    Requires a `folder` which contains:

    1. `ponencia.html`
    2. `fallo.html`
    3. `annex.html`
    """

    folder: Path

    @property
    def result(self):
        """Add a header on top of the text, then supply the body of the ponencia,
        followed by the fallo and the annex of footnotes."""
        txt = "# Ponencia\n\n"

        if base := self.convert_html_content("ponencia.html"):
            txt += f"{base.strip()}"

        if fallo := self.convert_html_content("fallo.html"):
            txt += f"\n\n{fallo.strip()}"

        if annex := self.convert_html_content("annex.html"):
            annex_footnoted = MD_FOOTNOTE_AFTER_LEFT_RIGHT.sub(": ", annex)
            txt += f"\n\n{annex_footnoted.strip()}"

        return txt.strip()

    def convert_html_content(self, htmlfilename: str) -> str | None:
        p = self.folder / f"{htmlfilename}"
        if p.exists():
            html_content = p.read_text()
            md_content = self.from_html_to_md(html_content)
            return md_content
        return None

    def revise_md_footnotes(self, raw: str):
        partial = MD_FOOTNOTE_LEFT.sub("[^", raw)
        replaced = MD_FOOTNOTE_RIGHT_POST_LEFT.sub("]", partial)
        return replaced

    def from_html_to_md(self, raw: str) -> str:
        options = dict(
            sup_symbol="^",  # footnote pattern which will be replaced
            escape_asterisks=False,
            escape_underscores=False,
        )
        initial_pass = markdownify(raw, **options)
        revised_pass = self.revise_md_footnotes(initial_pass)
        return revised_pass


def add_markdown_file(p: Path, text: str, key: str):
    opinions_path = p / "opinions"
    opinions_path.mkdir(exist_ok=True)
    ponencia_path = opinions_path / f"{key}.md"
    # if not ponencia_path.exists():
    ponencia_path.write_text(text)
