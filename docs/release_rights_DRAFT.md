# Rights statement for the Dataverse deposit — DRAFT, needs Hanno's decision

**Status:** draft. Not agreed. This is the last open item before release.

## The question

We want to publish the full scraped text of candidate campaign websites. Those
pages were written by campaigns and are, in principle, copyrighted. Pons was
asked about this on 2026-07-05 and did not answer that part of the question, so
the decision is ours.

## The precedent, which is stronger than I first thought

ICPSR 226001 **does** distribute the full scraped website text — `websites_clean`
is text, not scores — and their README §3.1 states:

> "The authors confirm that they hold the rights to publish all data included in
> this replication package, except for data explicitly excluded (i.e., the French
> Agenda Project and the Manifesto Project Database). All other data are either
> produced by the authors, available in the public domain, or distributed under
> licenses that allow republication for academic purposes."

So a peer-reviewed AEA-linked deposit of exactly this material, from the same
source (Internet Archive), already exists under CC BY 4.0. That is a direct
precedent and it survived openICPSR's own review.

## What I recommend

**Mirror their posture, and be more explicit about provenance than they were.**

Three components:

1. **Licence the dataset CC BY 4.0**, matching openICPSR 226001. This licenses
   *our* compilation, coding and derived variables. It does not purport to
   license the underlying campaign copy, and should not claim to.

2. **State the provenance explicitly.** Every row carries `snap_url`, so each
   observation is traceable to a public Internet Archive capture and can be
   re-fetched. We are redistributing an extract of already-public archival
   material for non-commercial academic research, not republishing campaign
   websites as such.

3. **Offer a takedown route.** A named contact and a commitment to remove
   material on a substantiated request from a rights-holder. Cheap, and it is
   what makes the posture defensible in practice rather than only in principle.

## Draft text for the deposit

> **Terms of use.** This dataset is released under a Creative Commons
> Attribution 4.0 International (CC BY 4.0) licence, which applies to the
> compilation, the derived variables, and the documentation.
>
> The website text is an extract of publicly archived pages captured by the
> Internet Archive's Wayback Machine. Each row records `snap_url`, the exact
> archived URL from which its text was taken, so every observation is traceable
> to its public source. The material is redistributed for non-commercial
> academic research.
>
> Copyright in the underlying campaign material remains with its authors. If you
> are a rights-holder and wish material removed, contact <address> and we will
> act on substantiated requests.

## What I am not able to tell you

I am not a lawyer and this is not legal advice. Two things worth weighing that I
cannot resolve:

- Whether UC Davis has a research-data or scholarly-communications office that
  should sign off. Most universities do, and a five-minute check there is
  cheaper than a later problem.
- Whether Harvard Dataverse imposes its own terms that interact with CC BY 4.0.
  Their deposit agreement should be read before upload.

## The alternative, if you want lower risk

Release the **derived variables and the roster** openly, and place the **full
text** behind a request form or a restricted-access tier that Dataverse
supports. This keeps the reproducibility benefit for the coded variables while
limiting bulk redistribution of the raw copy.

I do not recommend this as the default, because it is more cautious than the
existing ICPSR precedent requires and it substantially reduces the value of the
release — the text is the part nobody else has. But it is the option to take if
you want the conservative path.
