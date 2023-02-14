from citation_utils import Citation
from dateutil.parser import parse
from loguru import logger
from slugify import slugify


def get_cite_from_fields(data: dict) -> Citation | None:
    """Presumes existence of the following keys:

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
    return cite


def get_id_from_citation(
    folder_name: str,
    source: str,
    citation: Citation,
) -> str:
    """The decision id to be used as a url slug ought to be unique,
    based on citation paramters if possible.
    """
    if not citation.slug:
        logger.debug(f"Citation absent: {source=}; {folder_name=}")
        return folder_name

    if source == "legacy":
        return citation.slug or folder_name

    elif citation.docket:
        if report := citation.scra or citation.phil:
            return slugify("-".join([citation.docket, report]))
        return slugify(citation.docket)
    return folder_name
