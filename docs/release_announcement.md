# Release announcement drafts

Social posts for the v1 release. Not yet posted — the deposit does not exist,
so every version below needs a link before it goes out.

Coverage framing follows `hannohilbig.com/localparties/` (CLARA): state the
denominator, name where it is thinnest, and say what the data can and cannot
support. Concrete numbers rather than "coverage varies".

---

## Long post (default)

> Here is another dataset, this time congressional and Senate candidate websites in the US.
>
> It extends the ICPSR data by Pons et al backwards and forwards in time, and adds Senate candidates, who weren't covered before. Archived campaign website text for 10,601 candidate-years, around 1.18 million pages, all from the Wayback Machine.
>
> You get the full text, a candidate-year panel, and coded variables that merge directly into the ICPSR data — length, lexical diversity, word complexity, and 31 topic proportions computed with their own classifier. FEC ids for everyone, DIME ids for 81%.
>
> On coverage: we tried 18,250 candidate-years and got 10,601, so about 58%. Whether a candidate shows up depends on whether the campaign had a findable website and whether the Internet Archive captured it. It's thinnest in the early Senate years: 2002 has 72 candidates in the roster and 30 captured, while 2022 has 693 and 435. There's a roster file listing everyone we tried, so you can use that as the denominator instead of treating this as a census.
>
> Comparisons across candidates, and pooled years, are more reliable than year-to-year changes for a single candidate.
>
> [link]

---

## Short post (Bluesky / X, single)

> New dataset: archived campaign website text for US House and Senate candidates, 2002–2024.
>
> Extends Pons et al (ICPSR) forwards, backwards, and to the Senate. 10,601 candidate-years, 1.18M archived pages, with coded variables that merge straight into theirs.
>
> Coverage is about 58%, and a roster file lists everyone we tried, including the misses.
>
> [link]

---

## Thread version

**1/**
> Here is another dataset, this time congressional and Senate candidate websites in the US. Archived campaign website text for 10,601 candidate-years, 2002–2024, built from about 1.18 million pages in the Wayback Machine. [link]

**2/**
> It extends the ICPSR data by Pons et al both backwards and forwards in time, and adds Senate candidates, who weren't covered before. The coded variables use their own code, so the two datasets stack into one panel.

**3/**
> You get the full text, a candidate-year panel, and variables that merge directly into theirs: length, lexical diversity, word complexity, and 31 topic proportions from their classifier. FEC ids for everyone, DIME ids for 81%.

**4/**
> On coverage: we tried 18,250 candidate-years and got 10,601, about 58%. A candidate shows up only if the campaign had a findable website and the Internet Archive captured it. Neither is under our control.

**5/**
> It's thinnest in the early Senate years: 2002 has 72 candidates in the roster and 30 captured, 2022 has 693 and 435. The roster file lists everyone we tried, so you can use it as the denominator rather than treating this as a census.

**6/**
> Practical upshot: comparisons across candidates, and pooled years, are more reliable than year-to-year changes for a single candidate.

---

## Notes

**Do not claim it merges seamlessly without qualification.** It does now, but
only because the release replicates a text-cleaning step in the original
pipeline that is easy to miss. Skipping it inflates document length by about
half. That belongs in the documentation, not a post, but do not write anything
that implies the merge is trivial.

**Do not describe the topics as positions.** They are 31 Manifesto categories
measuring issue attention. Stance coding is a possible v2, not in this release.

**If asked why the German data (CLARA) withheld text and this does not:** GDPR
applies to the German case and the pages were written by party organisations.
Here the material is US campaign speech, already public through the Internet
Archive, and the deposit carries the source URL for every row. Two peer
projects release comparable verbatim text: ICPSR 226001, and CampaignView on
Harvard Dataverse under CC0.

**Numbers used above, all verified:** 10,601 captured candidate-years; 18,250
attempted; 1,175,582 page rows; Senate 2002 roster 72 / captured 30; Senate
2022 roster 693 / captured 435; FEC ids 100%; DIME ids 81%; 31 topic columns.
