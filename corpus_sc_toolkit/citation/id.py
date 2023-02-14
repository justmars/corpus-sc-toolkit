from loguru import logger
from citation_utils import Citation
from slugify import slugify


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
