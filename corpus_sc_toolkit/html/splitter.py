import re
from collections.abc import Iterator

single_spaced = re.compile(r"\s*\n\s*")
double_spaced = re.compile(r"\s*\n\s*\n\s*")


def remove_ponente_prefix(text: str) -> str:
    return text.removeprefix("# Ponencia")


def standardize(text: str):
    return (
        text.replace("\xa0", "")
        .replace("\xad", "-")
        .replace("“", '"')
        .replace("”", '"')
        .replace("‘", "'")
        .replace("’", "'")
        .strip()
    )


def segmentize(full_text: str, min_num_chars: int = 10) -> Iterator[dict]:
    """Split first by double-spaced breaks `\\n\\n` and then by
    single spaced breaks `\\n` to get the position of the segment.

    Will exclude footnotes and segments with less than 10 characters.

    Args:
        full_text (str): The opinion to segment

    Yields:
        Iterator[dict]: The partial segment data fields
    """
    if cleaned_text := standardize(full_text):
        if subdivisions := double_spaced.split(cleaned_text):
            for idx, text in enumerate(subdivisions):
                if lines := single_spaced.split(text):
                    for sub_idx, segment in enumerate(lines):
                        # --- marks the footnote boundary in # converter.py
                        if segment == "---":
                            return
                        position = f"{idx}-{sub_idx}"
                        char_count = len(segment)
                        if char_count > min_num_chars:
                            yield {
                                "position": position,
                                "segment": segment,
                                "char_count": char_count,
                            }
