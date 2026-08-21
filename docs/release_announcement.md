# Release announcement drafts

Harvard Dataverse has published Version 2.0. The text below refers to that
version.

## LinkedIn post

Here is another dataset: archived campaign website text for US House and Senate candidates. It contains ~800k pages and covers about 7,300 candidate-years. The dataset extends the House candidate corpus collected by Di Tella et al. (ICPSR 226001), which covers 2002–2016. My dataset covers House candidates from 2018–2024, and adds Senate election candidates from 2002–2024.

The data include extracted text from each page, along with a bunch of derived variables: FEC identifiers and measures of document length, lexical diversity, and topic attention for 31 topics. These measures follow Di Tella et al., so the two House panels can be combined and analyzed as one dataset.

Coverage is incomplete because the Wayback Machine archives pages selectively. Of 9,848 candidates with a recorded URL attempt, 7,353 have archived text. Among candidates in official general election returns, House coverage ranges from 69 to 76 percent from 2018 to 2024. Senate coverage rises from 17 percent in 2002 to about 70 percent after 2008.

Comparisons across candidates or pooled observations are likely more reliable than analyses of changes within units such as party-district cells.

I also ran some validation checks and included examples of analyses that can be conducted with these data. For example, the data can be used to track candidate attention to different topics, as well as changes in D-R attention differences over time. The D-R gap in welfare state attention increases from 1.5 percentage points in 2002 to 12.3 points in 2024. Another application is examining the amount of information provided by candidates on their websites. Descriptively, candidates in closer races tend to provide a larger set of page types than candidates in less competitive races.

Data: https://doi.org/10.7910/DVN/BZ2JRS

Documentation and findings: https://www.hannohilbig.com/candidatewebsites/

## Twitter/X thread

### 1/3

Here is another dataset: archived campaign website text for US House and Senate candidates. It contains ~800k pages and covers about 7,300 candidate-years. The dataset extends the House candidate corpus collected by Di Tella et al. (ICPSR 226001), which covers 2002–2016. My dataset covers House candidates from 2018–2024, and adds Senate election candidates from 2002–2024.

### 2/3

The data include extracted text from each page, along with a bunch of derived variables: FEC identifiers and measures of document length, lexical diversity, and topic attention for 31 topics. These measures follow Di Tella et al., so the two House panels can be combined and analyzed as one dataset.

Coverage is incomplete because the Wayback Machine archives pages selectively. Of 9,848 candidates with a recorded URL attempt, 7,353 have archived text. Among candidates in official general election returns, House coverage ranges from 69 to 76 percent from 2018 to 2024. Senate coverage rises from 17 percent in 2002 to about 70 percent after 2008.

### 3/3

Comparisons across candidates or pooled observations are likely more reliable than analyses of changes within units such as party-district cells.

I also ran some validation checks and included examples of analyses that can be conducted with these data. For example, the data can be used to track candidate attention to different topics, as well as changes in D-R attention differences over time. The D-R gap in welfare state attention increases from 1.5 percentage points in 2002 to 12.3 points in 2024. Another application is examining the amount of information provided by candidates on their websites. Descriptively, candidates in closer races tend to provide a larger set of page types than candidates in less competitive races.

Data: https://doi.org/10.7910/DVN/BZ2JRS

Documentation and findings: https://www.hannohilbig.com/candidatewebsites/

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
