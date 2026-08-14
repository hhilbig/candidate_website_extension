# Rights statement for the Dataverse deposit

**Status: DECIDED 2026-08-13 (Hanno), amended 2026-08-14.** CC BY 4.0 on our
compilation, explicit `snap_url` provenance, and a takedown route. The final
wording is in `docs/RELEASE_README.md`.

The 2026-08-13 entry described this as mirroring ICPSR 226001. That was wrong,
and the correction matters for how the posture is defended. Reading their README
directly on 2026-08-14 shows it contains **no takedown route, no contact, and no
mention of copyright in the underlying material at all** — the words takedown,
removal, contact and copyright do not appear. Their entire rights text is the
§3.1 assertion quoted below. So our posture is not a match to the precedent; it
goes beyond it on the one point that costs us nothing.

The contact question is settled: Harvard Dataverse gives every dataset a
**Contact Owner** control that relays a message to the depositor without
publishing an address. The clause points at that, so no separately monitored
address goes on a public page.

Outstanding: the UC Davis research-data/scholarly-communications check. That
moved from "worth doing" to "the thing to do" once Pons confirmed no legal
review sat behind the precedent.

## The question

We want to publish the full scraped text of candidate campaign websites. Those
pages were written by campaigns and are, in principle, copyrighted. Pons was
asked about this on 2026-07-05 and did not answer that part of the question, so
the decision is ours.

## Pons's answer (2026-08-13)

Asked directly whether he ran the website text past a research data office or
legal, he replied:

> "We did not involve anyone on the legal side."

So the rights assertion in their README §3.1 is the authors' own judgment. It is
not a cleared or reviewed position. The precedent below is real — the deposit is
public, licensed CC BY 4.0, and passed openICPSR intake — but it carries no legal
review behind it, and it should not be treated as though it does.

## The precedent, weaker still on a direct read

ICPSR 226001 **does** distribute the full scraped website text — `websites_clean`
is text, not scores — and their README §3.1 states:

> "The authors confirm that they hold the rights to publish all data included in
> this replication package, except for data explicitly excluded (i.e., the French
> Agenda Project and the Manifesto Project Database). All other data are either
> produced by the authors, available in the public domain, or distributed under
> licenses that allow republication for academic purposes."

That is the whole of it. There is no acknowledgement that campaign pages are
copyrighted by the campaigns, no removal route, and no contact. The two
exclusions they do make (the French Agenda Project and the Manifesto Project
Database) are both cases where a *data provider* imposed redistribution terms,
not cases about the scraped website text.

So a peer-reviewed AEA-linked deposit of exactly this material, from the same
source (Internet Archive), already exists under CC BY 4.0 and passed openICPSR
intake. That is a real precedent for what a repository will accept. It is not
evidence that anyone with legal training looked at it — Pons has confirmed
nobody did — and it is not a model for how to word the rights section, because
on this question it says nothing.

## The decision

**Take their licence, and go further than they did on provenance and on
recourse.**

Three components:

1. **Licence the dataset CC BY 4.0**, matching openICPSR 226001. This licenses
   *our* compilation, coding and derived variables. It does not purport to
   license the underlying campaign copy, and should not claim to.

2. **State the provenance explicitly.** Every row carries `snap_url`, so each
   observation is traceable to a public Internet Archive capture and can be
   re-fetched. We are redistributing an extract of already-public archival
   material for non-commercial academic research, not republishing campaign
   websites as such.

3. **Offer a takedown route.** A commitment to remove material on a
   substantiated request from a rights-holder, reachable through Dataverse's own
   Contact Owner control. ICPSR 226001 offers nothing equivalent. It is cheap,
   it needs no address that someone has to remember to monitor, and it is what
   makes the posture defensible in practice rather than only in principle.

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
> are a rights-holder and wish material removed, use the Contact Owner control on
> this dataset's Dataverse record. Substantiated requests will be acted on.

## What I am not able to tell you

I am not a lawyer and this is not legal advice. Two things worth weighing that I
cannot resolve:

- **UC Davis research-data / scholarly-communications sign-off. Now the main
  open item.** Pons confirmed (2026-08-13) that no legal review sat behind the
  ICPSR deposit, so mirroring it means relying on a peer's unreviewed judgment.
  That may well be correct — campaign websites are political speech meant for
  public circulation, the pages are already public via the Internet Archive, and
  the use is non-commercial academic research — but it is a judgment, not a
  clearance, and the library check is close to free.
- Whether Harvard Dataverse imposes its own terms that interact with CC BY 4.0.
  Their deposit agreement should be read before upload.

## The alternative that was considered and NOT taken

Release the **derived variables and the roster** openly, and place the **full
text** behind a request form or a restricted-access tier that Dataverse
supports. This keeps the reproducibility benefit for the coded variables while
limiting bulk redistribution of the raw copy.

Not taken. It is more cautious than the existing ICPSR precedent requires and it
substantially reduces the value of the release, since the text is the part
nobody else has. Recorded here so the choice is visible rather than implicit.
