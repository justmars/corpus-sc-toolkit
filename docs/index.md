# corpus-sc-toolkit

## Purpose

The library applies field extraction/validation to Philippine statutes and decisions.

It uploads a "source of truth" `yaml` file that can serve as future sqlite db entries on download and deserialization.

1. This is a compilation of several custom libraries, notably `citation-utils`, `statute-trees`, `sqlpyd`, `pylts`, etc.
2. `sqlpyd` defines an `TableConfig` which inheriting Pydantic models can use It makes creation of models / tables easier, especially for use in `sqlite_utils`
3. The data gathering part is tedious so it's necessary to store the model (and related source files like html) in remote file storage (`Cloudflare R2`) prior to database manipulation. The remote storage then becomes the single source of truth. Changes made to the remote file storage will need to be replicated in the database layer.

## Flow

### Identity

Uniform [identity](identity.md) of objects depending on context

Context | Separator | Example | Rationale
--:|:--:|:--:|:--:
r2 storage | backslash `/` | `gr/118289/1999/12/13/details.yaml` | The backslash is commonly used to "fake" appearance of folders in storage.
database | dot `.` | `gr.118289.1999.12.13` | A dash `-` conflicts with docket serial ids and statute category ids.
website | dash `-` | `gr-118289/1999-12-13/detail.html` | The URL pattern would make sense in grouping the date as string

## Storage

### Acquisition

- [x] Get PDF files from Supreme Court website
- [x] Get html files from Supreme Court e-library

### Unify Model
