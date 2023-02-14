import datetime
from typing import NamedTuple
from dateutil.parser import parse
from sqlite_utils.db import Table, Database

from loguru import logger
from .justice_table import Justice
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
        designation should be 'C.J.' or 'J.'"""
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
