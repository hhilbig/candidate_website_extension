# Release announcement drafts

The dataset was published on Harvard Dataverse on August 19, 2026. The landing
page and DOI below are live. Review the final post and attached figures before
publishing it on any platform.

## Long post

> I am releasing a dataset of archived campaign website text for U.S. House and
> Senate candidates. It contains 799,058 pages for 7,353 candidate-years.
>
> The dataset extends the House candidate corpus assembled by Di Tella et al.
> (ICPSR 226001). Their data cover House elections from 2002 through 2016. This
> release adds House elections from 2018 through 2024 and Senate elections from
> 2002 through 2024. The two datasets are designed to be appended by rows.
>
> The files include page-level text, a candidate-year panel, and measures that
> match the ICPSR data: document length, lexical diversity, and attention to 31
> Manifesto Project topics. FEC candidate IDs are available for every captured
> candidate-year; DIME IDs are available for 79 percent.
>
> Coverage is selective. Of 9,848 candidate-years with a recorded URL attempt,
> 7,353, or 75 percent, yielded archived text. Among general election ballot
> candidates, the capture rate is 84 percent. Among other same-election-year FEC
> candidates, it is 69 percent. The roster lists every attempted candidate, so
> researchers can choose and report the denominator that fits their design.
>
> The data also recover familiar partisan differences in topic attention. Among
> House general-election candidates, the Democratic-minus-Republican gap in
> welfare-state attention increases from 1.5 percentage points in 2002 to 12.3
> points in 2024. The same partisan topic profile appears in House and Senate
> campaigns from 2018 through 2024 (r = .92 across 31 topics).
>
> The release records uneven archival coverage directly. I would treat
> comparisons across candidates and pooled years as more reliable than
> within-candidate changes over time.
>
> https://www.hannohilbig.com/candidatewebsites/
> https://doi.org/10.7910/DVN/BZ2JRS

## Short post

> New dataset: 799,058 archived campaign-website pages for 7,353 U.S. House and
> Senate candidate-years. Extends ICPSR 226001, with page-level text, 31 topic
> measures, and a roster of 9,848 URL attempts.
> https://doi.org/10.7910/DVN/BZ2JRS

## Thread version

**1/**
> I am releasing a dataset of archived campaign website text for U.S. House and
> Senate candidates: 7,353 candidate-years and 799,058 pages from the Wayback
> Machine. https://www.hannohilbig.com/candidatewebsites/

**2/**
> The dataset extends the House candidate corpus assembled by Di Tella et al.
> (ICPSR 226001). Their data cover House elections from 2002 through 2016. This
> release adds House elections from 2018 through 2024 and Senate elections from
> 2002 through 2024. The two datasets are designed to be combined.

**3/**
> The files include page-level text, a candidate-year panel, and measures that
> match the ICPSR data: length, lexical diversity, and attention to 31 Manifesto
> Project topics. FEC candidate IDs are available for every captured
> candidate-year and DIME IDs for 79 percent.

**4/**
> Of 9,848 candidate-years with a recorded URL attempt, 7,353, or 75 percent,
> yielded archived text. The capture rate is 84 percent among general election
> ballot candidates and 69 percent among other same-election-year FEC
> candidates.

**5/**
> Among House general-election candidates, the Democratic-minus-Republican gap
> in welfare-state attention increases from 1.5 percentage points in 2002 to
> 12.3 in 2024. Topic attention measures emphasis, not policy position.

**6/**
> The partisan topic profile also appears in both chambers from 2018 through
> 2024: House and Senate gaps correlate at .92 across 31 topics. I would treat
> pooled comparisons as more reliable than within-candidate changes over time.

## Figure captions

**Long-run partisan issue gaps**

> Democratic minus Republican topic attention among House general-election
> candidates. The band marks the change from Di Tella et al. (2002–2016) to
> this release (2018–2024).

**Coverage against the ballot**

> Share of Democratic and Republican general-election candidates with captured
> websites. Dashed lines weight candidates by votes received.

**The 2024 party agendas**

> Topic attention among Democratic and Republican House general-election
> candidates in 2024. Topics measure emphasis, not policy position.

**House–Senate replication**

> Democratic minus Republican topic-attention gaps in House and Senate
> campaigns, 2018–2024. Across 31 topics, the two profiles correlate at .92.

## Posting notes

- Use the four-figure carousel in this order: long-run partisan issue gaps,
  coverage against the ballot, the 2024 party agendas, and House–Senate
  replication.
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
