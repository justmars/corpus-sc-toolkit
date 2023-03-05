# Statutes

## Statute Extraction

The [statute-trees](https://github.com/justmars/statute-trees) and [statute-patterns](https://github.com/justmars/statute-patterns) libraries are components required to extract data from a path and create a unified `details.yaml`. This file can be uploaded to R2 using the `StatuteUploadedPage` model.

```py title="With a statutes directory"
>>> from corpus_sc_toolkit import StatuteUploadedPage
>>> from pathlib import Path
>>> p = Path.home() / <insert path here which contains relevant statute files>
>>> StatuteUploadedPage.from_details(p) # will upload to R2 bucket: ph-statutes
```
