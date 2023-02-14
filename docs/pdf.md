# Extracts

## Source of Extraction

There is a pre-existing sqlite database that is replicated via litestream to the repository. This database, found in `s3://corpus-pdf/db` refers to content [previously extracted from pdf files](https://github.com/justmars/corpus-extractor).

```py
>>> from pylts import ConfigS3
>>> from pathlib import Path
>>> from sqlpyd import Connection
>>> stream = ConfigS3(s3='s3://corpus-pdf/db', folder=Path().cwd() / "data")
# stream.restore()
>>> c = Connection(DatabasePath=str(stream.dbpath), WAL=True) # database access
```

## Extraction Flow

### Function

Based on an SQL expression, extract pydantic models based on the following function:

::: corpus_sc_toolkit.pdf.decision.InterimDecision.limited_decisions

See raw SQL expression below that needs to be instantied into their respespective Decision, Opinion, and Segment models.

### SQL

We can query the database with the following SQL expression to get all "non-notices" with their respective opinions:

```sql
WITH opinions_included AS (
  SELECT
    op.id,
    op.pdf,
    op.title,
    op_meta.writer,
    op_meta.body opinion_body,
    op_meta.annex opinion_annex
  FROM
    pre_tbl_opinions op
    JOIN pre_tbl_opinion_meta op_meta
    ON op_meta.opinion_id = op.id
  WHERE
    op.category = caso.category
    AND op.serial = caso.serial
    AND op.date = caso.date
),
opinion_list_data AS (
  SELECT
    json_group_array(
      json_object(
        'id',
        op_inc.id,
        'pdf',
        op_inc.pdf,
        'title',
        op_inc.title,
        'writer',
        op_inc.writer,
        'body',
        op_inc.opinion_body,
        'annex',
        op_inc.opinion_annex
      )
    ) opinion_list
  FROM
    opinions_included op_inc
),
opinions_with_ponencia AS (
  SELECT
    json_insert(
      (
        SELECT
          opinion_list
        FROM
          opinion_list_data
      ),
      '$[#]',
      json_object(
        'id',
        caso.id,
        'pdf',
        caso.pdf,
        'title',
        CASE meta.notice
          WHEN 1 THEN 'Notice'
          WHEN 0 THEN 'Ponencia'
        END,
        'writer',
        meta.writer,
        'body',
        meta.body,
        'annex',
        meta.annex
      )
    ) opinions
)
SELECT
  caso.scraped,
  caso.id,
  caso.title,
  caso.category docket_category,
  caso.serial,
  caso.date,
  caso.pdf,
  meta.composition,
  meta.notice,
  meta.category,
  (
    SELECT
      opinions
    FROM
      opinions_with_ponencia
  ) opinions
FROM
  pre_tbl_decisions caso
  JOIN pre_tbl_decision_meta meta
  ON meta.decision_id = caso.id
WHERE
  meta.notice = 0
```
