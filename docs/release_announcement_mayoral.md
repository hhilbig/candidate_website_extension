# Mayoral release announcement drafts

Draft for the mayoral dataset (version 6). Not posted. Add the Dataverse link after publication.

## LinkedIn post

Here is another dataset: archived campaign websites of mayoral candidates in the 100 largest US cities, 2010–2026. For each election, it includes the winner and the runner-up. In cities with partisan primaries, it also includes the top two candidates of the decisive primary. The data cover 430 elections and contain about 46,000 archived pages.

Of the 860 finalists, 83 percent have usable website text: 88 percent of winners and 77 percent of runners-up. For 71 percent of the elections, we have text for both finalists. Each page records whether it was archived before or after election day and, in two-round races, before or after the first round. The data also link each finalist to the American Local Government Elections Database (ALGED).

One simple use is tracking how campaign websites changed. Between 2010–13 and 2022–25, the share of finalists whose website has an endorsements page rose from 24 to 42 percent, and the share with a biography page rose from 45 to 57 percent.

Data: [Dataverse link]

## Twitter/X thread

### 1/2

Here is another dataset: archived campaign websites of mayoral candidates in the 100 largest US cities, 2010–2026. It includes the top two candidates of each election, plus the top two of the decisive primary in cities with partisan primaries. 430 elections, about 46,000 pages.

### 2/2

83 percent of finalists have website text (88 percent of winners, 77 percent of runners-up), and 71 percent of elections have text for both. Pages are flagged by campaign period, and finalists link to ALGED.

Data: [Dataverse link]

## Figure captions

**Coverage by year**

> Share of mayoral finalists (winner and runner-up of each decisive election in the
> 100 largest US cities) with usable archived website text, by election year. 2026
> covers elections up to September 14.

**Campaign website pages**

> Share of finalists with website text whose archived website has at least one page of
> each type, by election period. Page types come from page addresses. The Wayback
> Machine archived more pages of recent websites, which can contribute to the increase.

## Posting checks

- Figures: `quality_reports/social_figure_candidates/mayoral/figures/` (PDF and PNG), built by
  `scripts/mayoral_social_figure_data.py` and `scripts/mayoral_social_figures.R` from the release folder.
- Every number above comes from released columns (`text_status`, `official_result_role`, `page_type`,
  `release_usable`).
