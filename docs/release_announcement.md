# Release announcement drafts

These drafts are not ready to post until the Dataverse record exists. Replace
`<DATAVERSE_URL>` with the final record URL before posting.

## Long post

> I am releasing a dataset of archived campaign website text for US House and
> Senate candidates.
>
> The dataset extends the House candidate corpus assembled by Di Tella et al.
> (ICPSR 226001). It adds House elections from 2018 through 2024 and Senate
> elections from 2002 through 2024. The release contains 7,353 captured
> candidate-years and 799,058 archived pages from the Wayback Machine.
>
> The files include page-level text, a candidate-year panel, and measures that
> can be appended to the ICPSR data: document length, lexical diversity, and
> attention to 31 Manifesto Project topics. FEC candidate IDs
> are available for every candidate-year; DIME IDs are available for 79 percent.
>
> Coverage is selective. Of 9,848 candidate-years with a recorded URL attempt,
> 7,353, or 75 percent, yielded archived text. Among general election ballot
> candidates, the capture rate is 84 percent. Among other same-election-year FEC
> candidates, it is 69 percent. The roster lists every attempted candidate, so
> researchers can choose and report the denominator that fits their design.
>
> Coverage is thinnest in the early Senate years: 13 of 28 candidates were
> captured in 2002, compared with 237 of 337 in 2022. Comparisons across
> candidates and pooled years are more reliable than year-to-year changes for a
> single candidate.
>
> <DATAVERSE_URL>

## Short post

> New dataset: archived campaign website text for US House and Senate
> candidates, 2002–2024. It extends ICPSR 226001 with 7,353 candidate-years and
> 799,058 Wayback Machine pages. The release includes the full text, coded
> variables, and a roster of all 9,848 URL attempts. Overall capture is 75
> percent. <DATAVERSE_URL>

## Thread version

**1/**
> I am releasing a dataset of archived campaign website text for US House and
> Senate candidates: 7,353 candidate-years and 799,058 pages from the Wayback
> Machine. <DATAVERSE_URL>

**2/**
> The dataset extends the House candidate corpus assembled by Di Tella et al.
> (ICPSR 226001). It adds House elections from 2018 through 2024 and Senate
> elections from 2002 through 2024.

**3/**
> The files include page-level text, a candidate-year panel, and measures that
> can be appended to the ICPSR data: length, lexical diversity, and attention to
> 31 Manifesto Project topics. FEC candidate IDs are available
> for all rows and DIME IDs for 79 percent.

**4/**
> Of 9,848 candidate-years with a recorded URL attempt, 7,353, or 75 percent,
> yielded archived text. The capture rate is 84 percent among general election
> ballot candidates and 69 percent among other same-election-year FEC
> candidates.

**5/**
> Coverage is thinnest in the early Senate years: 13 of 28 candidates were
> captured in 2002, compared with 237 of 337 in 2022. The roster lists every
> attempted candidate and supports alternative coverage denominators.

**6/**
> I would treat comparisons across candidates and pooled years as more reliable
> than year-to-year changes for a single candidate.

## Posting notes

- Do not describe the topic measures as positions. They measure attention to 31
  Manifesto Project categories.
- Do not describe the corpus as a census or as a sample limited to general
  election candidates. Of the 7,353 captured candidate-years, 3,032 match a
  Democratic or Republican general election ballot; 4,321 are other
  same-election-year FEC candidates.
- Do not imply that appending the two datasets requires no preparation. The
  release reproduces an easily missed text-cleaning step from the original
  pipeline; the documentation explains it.
- The German CLARA release withheld text because of its different legal and
  institutional setting. This release contains US campaign speech archived by
  the Internet Archive and records the source URL for every row. CC BY 4.0
  covers the compilation, coded variables, and documentation. Copyright in the
  underlying campaign text remains with its authors.

## Verified figures

Verified against the rebuilt release on 2026-08-18:

| Quantity | Value |
|---|---:|
| Captured candidate-years | 7,353 |
| Candidate-years with a URL attempt | 9,848 |
| Archived page rows | 799,058 |
| Captured general election candidates | 3,032 |
| Captured other same-election-year FEC candidates | 4,321 |
| Overall capture rate | 74.7% |
| Ballot candidate capture rate | 83.7% |
| Other same-year candidate capture rate | 69.4% |
| House ballot coverage, this release (2018–2024) | 69–76% |
| House ballot coverage, ICPSR 226001 (2002–2016) | 45–67% |
| Senate 2002, captured / attempted | 13 / 28 |
| Senate 2022, captured / attempted | 237 / 337 |
| Candidate-years with a DIME ID | 5,774 (78.5%) |
| Topic variables | 31 |
