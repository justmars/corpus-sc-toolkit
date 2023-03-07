# corpus-sc-toolkit

## Purpose

The library applies fields extraction and validation to Philippine statutes and decisions, uploads "source of truth" `yaml` file for each in R2 storage.

## Flow

1. Uniform [identity](identity.md) of objects in storage and in the database

## Technical Notes

1. This is a compilation of several other custom libraries, notably `citation-utils`, `statute-trees`, `sqlpyd`, `pylts`, etc.
2. `sqlpyd` defines an `TableConfig` which inheriting Pydantic models can use It makes creation of models / tables easier, especially for use in `sqlite_utils`
3. The data gathering part is tedious so it's necessary to store the model (and related source files like html) in remote file storage (`Cloudflare R2`) prior to database manipulation. The remote storage then becomes the single source of truth. Changes made to the remote file storage will need to be replicated in the database layer.
