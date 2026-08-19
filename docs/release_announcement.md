# Release announcement drafts

Harvard Dataverse published version 1.0 on August 19, 2026. Replace it with the
prepared version 1.1 before posting the text below.

## Long post

> I am releasing a dataset of archived campaign website text for U.S. House and
> Senate candidates. It contains 799,058 pages for 7,353 candidate-years.
>
> The dataset extends the House candidate corpus assembled by Di Tella et al.
> (ICPSR 226001). Their data cover House elections from 2002 through 2016. This
> release adds House elections from 2018 through 2024 and Senate elections from
> 2002 through 2024. The candidate-year panels are designed to be appended.
>
> The files include page-level text, a candidate-year panel, FEC identifiers,
> and measures of document length, lexical diversity, and attention to 31
> Manifesto Project topics. DIME IDs are available for 79 percent of captured
> candidate-years.
>
> Of 9,848 candidate-years with a recorded URL attempt, 7,353 yielded archived
> text. Capture rates are 84 percent among general-election candidates and 69
> percent among other same-election-year FEC candidates. The roster records the
> denominator for each group.
>
> Among House general-election candidates, the Democratic-minus-Republican gap in
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

**Race competitiveness and website development**

> Share of captured House campaign websites with at least three of five
> dedicated page types. The sample contains 1,977 candidates in two-party
> general-election races from 2018 through 2024. The relationship is
> descriptive.

## Posting checks

- Use the four captions above in carousel order.
- Topic measures capture attention, not policy positions.
- The corpus is neither a census nor a ballot-only sample: 3,032 captured
  candidate-years match a major-party general-election ballot and 4,321 do not.
- Appending the ICPSR data requires the replicated text filter described in the
  documentation.

## Verified figures

Verified against the version 1.1 build on 2026-08-20:

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
| Candidate-years with a DIME ID | 5,774 (78.5%) |
| Topic variables | 31 |
