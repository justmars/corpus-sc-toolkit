# corpus-sc-toolkit

The library applies fields extraction and validation to Philippine decisions (SC) and statutes and then uploads a "source of truth" `yaml` file in R2 storage.

Prefatorily, the most important problem in converting legal-paper based constructs to digital media, to me, is the problem of identity. How do we identify (and later on) refer to documents and citations? Since we need to store information in a remote facility, using _Amazon S3_ / _Cloudflare R2_ prefix conventions seems a worthy option to consider.

## prefix-based decisions

Example prefix `gr/118289/1999/12/13`, in `sc-decisions` R2 bucket, when deconstructed:

Key | Value | Description
--:|:-- |:--
title | _Trans-Asia Phils. Employees Association (Tapea) And Arnel Galvez, Petitioners, Vs. National Labor Relations Commission, Trans-Asia (Phils.) And Ernesto S. De Castro, Respondents._ | The title of the case
docket_category | GR | The part of the docket citation indicating the category
docket_id | 118289 | The serial id of the docket
docket_date | 1999-12-13 | The date found in the docket citation
report_phil | 378 Phil. 300 | The report citation for Philippine Reports

When downloaded, `gr/118289/1999/12/13/details.yaml` contains [fields](decisions/fields.md), detected opinions.

## prefix-based statutes

Another R2 bucket hosts `ph-statutes`. RA 386, as published, is `ra/1949/6/386/1` where:

Key | Value | Note
--:|:-- |:--
title | Republic Act No. 386 | Represents the serialized title, in long form
description | _An Act to Ordain and Institute the Civil Code of the Philippines_ | Official title of the Statute
category | ra | The statutory category in short form
serialid | 386 | The serial id of the category
variant | 1 | The suffix `/1` helps prevent duplicate titles (e.g. categories _rule_am_ and _rule_bm_)
date | 1949-06-18 | The year and month are included in the prefix: e.g. `1949/6`

The full `ra/1949/6/386/1/details.yaml` is downloadable, contain metadata including nested provisions.

1. individual segments;
2. an index of detected citations; and
3. an index of detected statutes.
