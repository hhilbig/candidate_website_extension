# Release announcement drafts

Harvard Dataverse has published Version 2.0. The text below refers to that
version.

## Long post

> I am releasing a dataset of archived campaign website text for U.S. House and
> Senate candidates. It contains 799,058 pages for 7,353 candidate-years.
>
> The dataset extends the House candidate corpus assembled by Di Tella et al.
> (ICPSR 226001). Their data cover House elections from 2002 through 2016. This
> release adds House elections from 2018 through 2024 and Senate elections from
> 2002 through 2024. The House panels can be appended to form one long panel.
>
> The files include text from each page, a panel by candidate and year, FEC
> identifiers, and measures of document length, lexical diversity, and attention to 31
> Manifesto Project topics. DIME IDs are available for 79 percent of captured
> candidate-years.
>
> Of 9,848 candidate-years with a recorded URL attempt, 7,353 yielded archived
> text. Capture rates are 84 percent among candidates in general elections and 69
> percent among other same-year FEC candidates. The roster records the
> denominator for each group.
>
> Among House candidates in general elections, the Democratic minus Republican
> gap in welfare state attention increases from 1.5 percentage points in 2002 to 12.3
> points in 2024. The same partisan topic profile appears in House and Senate
> campaigns from 2018 through 2024 (r = .92 across 31 topics).
>
> The release records uneven archival coverage directly. I would treat
> comparisons across candidates and pooled years as more reliable than
> changes within candidates over time.
>
> https://www.hannohilbig.com/candidatewebsites/
> https://doi.org/10.7910/DVN/BZ2JRS

## Short post

> New dataset: 799,058 archived campaign website pages for 7,353 U.S. House and
> Senate candidate-years. Extends ICPSR 226001, with text from each page, 31 topic
> measures, and a roster of 9,848 URL attempts.
> https://doi.org/10.7910/DVN/BZ2JRS

## Figure captions

**Partisan issue gaps over time**

> Democratic minus Republican topic attention among House candidates in general
> elections. The band marks the change from Di Tella et al. (2002–2016) to
> this release (2018–2024).

**Coverage against the ballot**

> Share of Democratic and Republican candidates in general elections with
> captured websites. Dashed lines weight candidates by votes received.

**The 2024 party agendas**

> Topic attention among Democratic and Republican House candidates in the 2024
> general election. Topics measure emphasis, not policy position.

**Race competitiveness and website development**

> Share of captured House campaign websites with at least three of five
> dedicated page types. The relationship is descriptive.

## Posting checks

- Use the four captions above in carousel order.
- Topic measures capture attention, not policy positions.
- The corpus is neither a census nor limited to candidates in general elections:
  3,032 captured candidate-years match an official Democratic or Republican
  ballot and 4,321 do not.
- Appending the ICPSR data requires the replicated text filter described in the
  documentation.

## Verified figures

Verified against the Version 2.0 build on 2026-08-20:

| Quantity | Value |
|---|---:|
| Captured candidate-years | 7,353 |
| Candidate-years with a URL attempt | 9,848 |
| Archived page rows | 799,058 |
| Captured general election candidates | 3,032 |
| Captured other same-year FEC candidates | 4,321 |
| Overall capture rate | 74.7% |
| Ballot candidate capture rate | 83.7% |
| Other same-year candidate capture rate | 69.4% |
| Candidate-years with a DIME ID | 5,774 (78.5%) |
| Topic variables | 31 |
