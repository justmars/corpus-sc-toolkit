# Fetch Instance

## Establish database

A pre-existing sqlite database, found in `s3://corpus-pdf/db`, contains justices and decisions [extracted from pdf files](https://github.com/justmars/corpus-extractor). The database is replicated via the process below:

```py
>>> from dotenv import find_dotenv, load_dotenv
>>> from pylts import ConfigS3
>>> from pathlib import Path
>>> from sqlpyd import Connection
>>> load_dotenv(find_dotenv()) # ensure presence of env variables for litestream
>>> stream = ConfigS3(s3='s3://corpus-pdf/db', folder=Path().cwd() / "data")
>>> # stream.restore() # will download the database
>>> c = Connection(DatabasePath=str(stream.dbpath), WAL=True) # database access via `c.db`
```

## Interim Decisions

The pdf files from the database have not yet been replicated to R2 storage.

Initialize instances of an Interim Decision:

```py
>>> from corpus_sc_toolkit import InterimDecision
>>> interim_objs = InterimDecision.fetch(c.db) # raw data found in the database
>>> x = next(interim_objs) # x is an instance of InterimDecision
```

Each instance can now be dumped to a local file:

```py
>>> x.dump() # produces a file found in corpus_sc_toolkit/tmp/temp_pdf.yaml
('GR/2021/10/227403/pdf.yaml', # a target prefix to use
 PosixPath('/Users/mv/Code/corpus-toolkit/corpus_sc_toolkit/tmp/temp_pdf.yaml')) # temporary file
```

The dumped file may be uploaded to R2:

```py
>>> x.upload() # if the file already exists, will return False
False
>>> x.upload(override=True) # will update the existing prefix data
True # can now check R2 for the matching prefix in the bucket name with prefix GR/2021/10/227403/pdf.yaml
```

## Stored Decisions

Non-PDF decisions are stored in R2 and they can be parsed using `StoredDecision` class:

```py
>>> from corpus_sc_toolkit import StoredDecision
>>> temp_objs = StoredDecision.prefetch(dockets=["GR"], years=(1996,1997))
>>> y_obj = next(temp_objs) # instances are found in r2
>>> y = StoredDecision.make(y_obj, c.db) # y is an instance of StoredDecision
```

When specific prefix is identified:

```py
>>> from corpus_sc_toolkit import StoredDecision
>>> prefix = "GR/1999/6/95405/"
>>> z_obj = StoredDecision.get_detailed_opinions_from_storage(prefix=prefix)
>>> z = StoredDecision.make(z_obj, c.db) # z is an instance of StoredDecision
```
