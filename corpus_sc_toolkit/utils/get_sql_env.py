from jinja2 import Environment, PackageLoader, select_autoescape

sqlenv = Environment(
    loader=PackageLoader(package_name="corpus_sc_toolkit", package_path="sql"),
    autoescape=select_autoescape(),
)
"""Jinja2-based environment to get templates from a common path."""
