import datetime
from typing import NamedTuple

from dateutil.parser import parse
from loguru import logger
from sqlite_utils.db import Database, Table

from .justice_model import Justice
from .justice_name import OpinionWriterName


class CandidateJustice(NamedTuple):
    db: Database
    text: str | None = None
    date_str: str | None = None

    @property
    def valid_date(self) -> datetime.date | None:
        if not self.date_str:
            return None
        try:
            return parse(self.date_str).date()
        except Exception:
            return None

    @property
    def candidate(self) -> str | None:
        if name_found := OpinionWriterName.extract(self.text):
            if name_found.writer:
                return name_found.writer
        return None

    @property
    def table(self) -> Table:
        res = self.db[Justice.__tablename__]
        if isinstance(res, Table):
            return res
        raise Exception("Not a valid table.")

    @property
    def rows(self) -> list[dict]:
        """When selecting a ponente or voting members, create a candidate list of
        justices based on the `valid_date`.

        Examples:
            >>> import yaml
            >>> from pathlib import Path
            >>> from corpus_sc_toolkit.justice import Justice, get_justices_file
            >>> from sqlpyd import Connection
            >>> p = Path().cwd() / "tests" / "sc.yaml" # the test file
            >>> c = Connection(DatabasePath="test.db")
            >>> tbl = c.create_table(Justice)
            >>> res = c.add_records(Justice, yaml.safe_load(get_justices_file(p).read_bytes()))
            >>> search = CandidateJustice(db=c.db, date_str='Dec. 1, 1995')
            >>> print(search.rows) # since start date is greater than target date, record is included
            [{'id': 137, 'surname': 'panganiban', 'alias': None, 'start_term': '1995-10-05', 'inactive_date': '2006-12-06', 'chief_date': '2005-12-20'}, {'id': 136, 'surname': 'hermosisima', 'alias': 'hermosisima jr.', 'start_term': '1995-01-10', 'inactive_date': '1997-10-18', 'chief_date': None}, {'id': 135, 'surname': 'francisco', 'alias': None, 'start_term': '1995-01-05', 'inactive_date': '1998-02-13', 'chief_date': None}, {'id': 134, 'surname': 'mendoza', 'alias': None, 'start_term': '1994-06-07', 'inactive_date': '2003-04-05', 'chief_date': None}, {'id': 133, 'surname': 'kapunan', 'alias': None, 'start_term': '1994-01-05', 'inactive_date': '2002-08-12', 'chief_date': None}, {'id': 132, 'surname': 'vitug', 'alias': None, 'start_term': '1993-06-28', 'inactive_date': '2004-07-15', 'chief_date': None}, {'id': 131, 'surname': 'puno', 'alias': None, 'start_term': '1993-06-28', 'inactive_date': '2010-05-17', 'chief_date': '2007-12-08'}, {'id': 128, 'surname': 'melo', 'alias': None, 'start_term': '1992-08-10', 'inactive_date': '2002-05-30', 'chief_date': None}, {'id': 127, 'surname': 'bellosillo', 'alias': None, 'start_term': '1992-03-03', 'inactive_date': '2003-11-13', 'chief_date': None}, {'id': 125, 'surname': 'romero', 'alias': None, 'start_term': '1991-10-21', 'inactive_date': '1999-08-01', 'chief_date': None}, {'id': 124, 'surname': 'davide', 'alias': 'davide jr.', 'start_term': '1991-01-24', 'inactive_date': '2005-12-20', 'chief_date': '1998-11-30'}, {'id': 123, 'surname': 'regalado', 'alias': None, 'start_term': '1988-07-29', 'inactive_date': '1998-10-13', 'chief_date': None}, {'id': 116, 'surname': 'padilla', 'alias': None, 'start_term': '1987-01-12', 'inactive_date': '1997-08-22', 'chief_date': None}, {'id': 115, 'surname': 'feliciano', 'alias': None, 'start_term': '1986-08-08', 'inactive_date': '1995-12-13', 'chief_date': None}, {'id': 112, 'surname': 'narvasa', 'alias': None, 'start_term': '1986-04-10', 'inactive_date': '1998-11-30', 'chief_date': '1991-12-08'}]
            >>> c.path_to_db.unlink() # tear down

        Returns:
            list[dict]: Filtered list of justices
        """  # noqa: E501
        if not self.valid_date:
            return []
        criteria = "inactive_date > :date and :date > start_term"
        params = {"date": self.valid_date.isoformat()}
        results = self.table.rows_where(
            where=criteria,
            where_args=params,
            select=(
                "id, lower(last_name) surname, alias, start_term,"
                " inactive_date, chief_date"
            ),
            order_by="start_term desc",
        )
        return list(results)

    @property
    def choice(self) -> dict | None:
        """Based on `get_active_on_date()`, match the cleaned_name to either the alias
        of the justice or the justice's last name; on match, determine whether the
        designation should be 'C.J.' or 'J.'

        Examples:
            >>> import yaml
            >>> from pathlib import Path
            >>> from corpus_sc_toolkit.justice import Justice, get_justices_file
            >>> from sqlpyd import Connection
            >>> p = Path().cwd() / "tests" / "sc.yaml" # the test file
            >>> c = Connection(DatabasePath="test.db")
            >>> tbl = c.create_table(Justice)
            >>> res = c.add_records(Justice, yaml.safe_load(get_justices_file(p).read_bytes()))
            >>> search = CandidateJustice(db=c.db, text='Panganiban, Acting Cj', date_str='Dec. 1, 1995')
            >>> print(search.choice) # note variance in text designation as acting CJ. vs. J.
            {'id': 137, 'surname': 'Panganiban', 'start_term': '1995-10-05', 'inactive_date': '2006-12-06', 'chief_date': '2005-12-20', 'designation': 'J.'}
            >>> # Note that the raw information above contains 'Acting Cj' and thus the designation is only 'J.' At present we only track 'C.J.' and 'J.' titles.
            >>> # With a different date, we can get the 'C.J.' designation.:
            >>> search_cj = CandidateJustice(db=c.db, text='Panganiban', date_str='2006-03-30')
            >>> print(search_cj.choice) # note variance in text designation as acting CJ. vs. J.
            {'id': 137, 'surname': 'Panganiban', 'start_term': '1995-10-05', 'inactive_date': '2006-12-06', 'chief_date': '2005-12-20', 'designation': 'C.J.'}
            >>> c.path_to_db.unlink() # tear down
        """  # noqa: E501
        opts = []
        if not self.valid_date:
            return None
        if not self.candidate:
            return None

        for candidate in self.rows:
            if candidate["alias"] and candidate["alias"] == self.candidate:
                opts.append(candidate)
                continue
            elif candidate["surname"] == self.candidate:
                opts.append(candidate)
                continue
        if opts:
            if len(opts) == 1:
                res = opts[0]
                res.pop("alias")
                res["surname"] = res["surname"].title()
                res["designation"] = "J."
                if chief_date := res.get("chief_date"):
                    s = parse(chief_date).date()
                    e = parse(res["inactive_date"]).date()
                    if s < self.valid_date < e:
                        res["designation"] = "C.J."
                return res
            else:
                logger.warning(
                    f"Many {opts=} for {self.candidate=} on {self.valid_date=}"
                )
        return None
