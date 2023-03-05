import re


def ascii_singleline(text: str):
    """S3 metadata can only contain ASCII characters."""
    text = re.sub(r"\r|\n", r" ", text)
    return re.sub(r"[^\x00-\x7f]", r"", text)
