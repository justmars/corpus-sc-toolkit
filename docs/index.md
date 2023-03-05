# corpus-sc-toolkit

Toolkit to process component elements of a Philippine Supreme Court decision.

## Metadata

```mermaid
flowchart TB
m(metadata)
m---format(general format)
format---html
format---pdf
m---dx(case title)
m---composition(court composition)
composition---eb(en banc: 15 justices)
composition---division(division: 5 justices)
m---cite(citation)
m---date(date promulgated)
```

## Citation

```mermaid
flowchart TB
cite(citation)---docket
cite---report
docket---d1(docket category)
docket---d2(docket serial)
docket---d3(docket date)
report---r1(phil report)
report---r2(scra)
report---r3(off. gaz.)
```

## Substructures

```mermaid
flowchart TB
decision---a(list of opinions)
a---mm(each opinion has its own metadata)
mm---writer(justice id)
mm---title(title of opinion)
mm---segments(each opinion may have a list of segments)
title--op(ponencia)
title--xconcur
title--xdissent
title--xseparate

```

## Decision Fields

::: corpus_sc_toolkit.decisions._resources.DecisionFields

## Decision Opinions

Each decision is divided into opinions:

::: corpus_sc_toolkit.decisions._resources.DecisionOpinion

## Opinion Segments

Each decision is divided into opinions:

::: corpus_sc_toolkit.decisions._resources.OpinionSegment
