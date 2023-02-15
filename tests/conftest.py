from pathlib import Path

import pytest
import yaml
from sqlpyd import Connection

from corpus_sc_toolkit import Justice

temppath = "tests/test.db"


@pytest.fixture
def justice_records(shared_datadir) -> list[dict]:
    f: Path = shared_datadir / "sc.yaml"
    return yaml.safe_load(f.read_bytes())


@pytest.fixture
def session(justice_records):
    c = Connection(DatabasePath=temppath)  # type: ignore
    c.create_table(Justice)
    c.add_records(Justice, justice_records)
    yield c.db
    c.db.close()  # close the connection
    Path().cwd().joinpath(temppath).unlink()  # delete the file
