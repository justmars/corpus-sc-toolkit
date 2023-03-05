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

### Originate from DB

Initialize instances of an Interim Decision:

```py
>>> from corpus_sc_toolkit import InterimDecision
>>> interim_objs = InterimDecision.originate(c.db) # raw data found in the database
>>> x = next(interim_objs) # x is an instance of an Interim Decision
>>> x.pdf_prefix # the target prefix property of an Interim Decision
'GR/2021/10/227403/pdf.yaml' # sample
```

### Dump to Local

Using the sample above, produce a temporary file found in `corpus_sc_toolkit/tmp/temp_pdf.yaml`:

```py
>>> x.dump() #
('GR/2021/10/227403/pdf.yaml',
 PosixPath('/Users/mv/Code/corpus-toolkit/corpus_sc_toolkit/tmp/temp_pdf.yaml'))
```

### Upload to R2

Instead of creating a dump file, can automatically upload the same to R2:

```py
>>> x.upload() # if the file already exists, will return False
False
>>> x.upload(override=True) # will update the existing prefix data
True # can now check R2 for the matching prefix in the bucket name with prefix GR/2021/10/227403/pdf.yaml
```

### Get from R2

After being uploaded, it can be recalled from R2, if we know the prefix:

```py
>>> output = InterimDecision.get(prefix="GR/2021/10/227403/pdf.yaml")
>>> type(output)
corpus_sc_toolkit.modes.interim.InterimDecision
```

## Raw Decisions

An initial set of "raw decisions" have previously been uploaded to R2. Note that these are unprocessed content. We can make an instance of a `RawDecision` which downloads and compiles such unprocessed content with:

```py
>>> from corpus_sc_toolkit import RawDecision
>>> temp_objs = RawDecision.prefetch(dockets=["GR"], years=(1996,1997)) # r2 filter
>>> y = next(temp_objs)
>>> type(y)
dict
```

When specific prefix is identified:

```py
>>> from corpus_sc_toolkit import RawDecision
>>> prefix = "GR/1999/6/95405/details.yaml"
>>> z_obj = RawDecision.preget(prefix=prefix)
>>> z = RawDecision.make(z_obj, c.db)
>>> type(z)
corpus_sc_toolkit.modes.raw.RawDecision
```
