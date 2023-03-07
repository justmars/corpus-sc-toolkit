# corpus-sc-toolkit

The library handles the processing of:

  1. Philippine Supreme Court Decisions; and
  2. Philippine rules in the form of Statutes and Codifications.

It extracts fields from raw content, uploading a "source of truth" `yaml` file in R2 storage.

## prefix-based decisions

Consider the following prefix `gr/1999/12/118289`, in the `sc-decisions` R2 bucket, where we can divine the following metadata at a glance:

Key | Value
--:|:--
category | Decision
composition | Division
docket_category | GR
docket_date | 1999-12-13
docket_id | 118289
id | gr.1999.12.118289
prefix | gr/1999/12/118289
report_phil | 378 Phil. 300
title | Trans-Asia Phils. Employees Association (Tapea) And Arnel Galvez, Petitioners, Vs. National Labor Relations Commission, Trans-Asia (Phils.) And Ernesto S. De Castro, Respondents.

The full `gr/1999/12/118289/details.yaml` file can be downloaded and will contain relevant [fields](decisions/fields.md) including detected opinions. Each opinion may contain:

1. individual segments;
2. an index of detected citations; and
3. an index of detected statutes.

## prefix-based statutes

Relatedly, a separate R2 bucket will host `ph-statutes`. So if we consider RA 386, as published, it's storage prefix would be `ra/1949/6/386/1`, the suffix `/1` takes into account the possibility of duplicate titles. This represents the following metadata:

Key | Value
--:|:--
category | ra
date | 1949-06-18
description |An Act to Ordain and Institute the Civil Code of the Philippines
id | ra.1949.6.386.1
prefix | ra/1949/6/386/1
serialid | 386
title | Republic Act No. 386
variant | 1

The full `ra/1949/6/386/1/details.yaml` file can be downloaded and will contain relevant metadata including nested provisions.

1. individual segments;
2. an index of detected citations; and
3. an index of detected statutes.
