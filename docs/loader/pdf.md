# Decision Loaded from PDF

## Source of Extraction

There is a pre-existing sqlite database that is replicated via litestream to the repository. This database, found in `s3://corpus-pdf/db` refers to content [previously extracted from pdf files](https://github.com/justmars/corpus-extractor).

```py
>>> from pylts import ConfigS3
>>> from pathlib import Path
>>> from sqlpyd import Connection
>>> stream = ConfigS3(s3='s3://corpus-pdf/db', folder=Path().cwd() / "data")
# stream.restore()
>>> c = Connection(DatabasePath=str(stream.dbpath), WAL=True) # database access
>>> c.db # An sqlite-utils Database instance
```

## Means of Generating Rows

```py
>>> from corpus_sc_toolkit import InterimDecision
>>> res = InterimDecision.limited_decisions(c.db)
>>> x = next(res) # x is an instance of InterimDecision
```

## Contents of Row from Path

::: corpus_sc_toolkit.loader.from_pdf.decision_from_pdf_db
