from pathlib import Path

import yaml
from loguru import logger
from statute_trees import StatutePage

from ..utils import create_temp_yaml
from ._resources import STATUTE_ORIGIN


class StatuteUploadedPage(StatutePage):
    ...

    @property
    def identitied(self) -> bool:
        """Check if a valid docket citation exists and return the same."""
        if not self.statute_category:
            return False

        if not self.statute_serial_id:
            return False

        if not self.variant:
            return False
        return True

    @property
    def base_prefix(self):
        if self.identitied:
            return "/".join(
                str(i)
                for i in [
                    self.statute_category,
                    self.statute_serial_id,
                    self.variant,
                    self.date,
                ]
            )
        raise Exception("Bad identity.")

    @property
    def meta(self):
        """When uploading to R2, the metadata can be included as extra arguments to
        the file."""
        reqs = [self.statute_category, self.statute_serial_id, self.date]
        if not any(reqs):
            return {}
        raw = {
            "Statute_Title": self.title,
            "Statute_Description": self.description,
            "Statute_Category": self.statute_category,
            "Statute_Serial_Id": self.statute_serial_id,
            "Statute_Date": self.date.isoformat(),
            "Statute_Variant": self.variant,
        }
        return {"Metadata": {k: str(v) for k, v in raw.items() if v}}

    @classmethod
    def from_details_to_temp_file(cls, details_path: Path):
        obj = cls.build(details_path)
        if not obj.identitied:
            logger.error(f"Bad identity {details_path=}")
            return None
        STATUTE_ORIGIN.upload(
            file_like=create_temp_yaml(obj.dict()),
            loc=f"{obj.base_prefix}/details.yaml",
            args=obj.meta,
        )
