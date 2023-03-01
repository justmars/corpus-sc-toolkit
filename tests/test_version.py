import pathlib

import tomllib

import corpus_sc_toolkit


def test_version():
    path = pathlib.Path("pyproject.toml")
    data = tomllib.loads(path.read_text())
    version = data["tool"]["poetry"]["version"]
    assert version == corpus_sc_toolkit.__version__
