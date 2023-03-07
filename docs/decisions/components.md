# Components

```mermaid
flowchart TB
decision---a(list of opinions)
a---mm(each opinion has its own metadata)
mm---writer(justice id)
mm---title(title of opinion)
mm---substructures(each opinion can consist of)
substructures---segments(subdivided text)
substructures---citations(a citation index)
substructures---statutes(a statute index)
title--op(ponencia)
title--xconcur
title--xdissent
title--xseparate
```

## Decision Opinions

Each decision is divided into opinions:

::: corpus_sc_toolkit.decisions.decision_opinions.DecisionOpinion

## Opinion Segments

Each decision is divided into opinions:

::: corpus_sc_toolkit.decisions.decision_opinion_segments.OpinionSegment
