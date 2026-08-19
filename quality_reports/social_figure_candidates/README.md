# Figure candidates for the public release

## Recommendation

Lead with the long-run partisan issue gaps, then use ballot coverage and the
House–Senate replication as validation. The 2024 agenda comparison is the best
standalone follow-up post because readers can interpret it without learning a
new statistic.

All substantive comparisons use Democratic and Republican general-election
candidates. The long-run House figures append 3,988 candidate-years from Di
Tella et al. (2002–2016) to 2,491 from the new release (2018–2024). The grey
band marks the change in source. No line connects observations across that
boundary.

Topic shares measure attention, not policy positions. They aggregate valid
archived pages for each candidate-year. They do not use only the single text
snapshot selected for the candidate-year panel.

## Ranked proposals

| Rank | Figure | Main result | Use |
|---:|---|---|---|
| 1 | Long-run partisan issue gaps | The Democratic–Republican welfare gap grows from 1.5 points in 2002 to 12.3 in 2024. Equality also moves toward Democrats; military and law-and-order attention move toward Republicans. | Lead figure. Interesting and useful as a boundary check. |
| 2 | Coverage against the official ballot | House candidate coverage is 69–76% in 2018–2024; vote-weighted coverage is about four points higher. | Main transparency and validation figure. |
| 3 | House–Senate replication | Party-gap profiles correlate at .923 across 31 topics and have the same sign for 28 topics. | Strongest validation that the content pattern is not specific to House campaigns. |
| 4 | 2024 party agendas | Democrats devote 17.8% of attention to welfare, compared with 5.5% for Republicans. Republicans devote more attention to political authority, law and order, and the military. | Best simple standalone social post. |
| 5 | Party-agenda distance | The share of attention that would need to be reallocated to align the parties rises from 20.8% in 2018 to 27.8% in 2024. | Interesting follow-up; define the measure in the post. |
| 6 | Homepage replication | Full-site and homepage party gaps correlate at .867 and have the same sign for 26 of 31 topics. | Shows that deep-page crawl composition does not create the main party pattern. |
| 7 | Website length by party | Median House website length rises for both parties between 2002 and 2024. The Democratic–Republican difference narrows from 61 to 7 words. | A non-topic result and a cross-source comparability check. |
| 8 | Homepage versus full site | Homepages contain 3.9 points less welfare attention and more identity and broad political language than full sites. | Interesting, but the descriptive interpretation is less secure because page roles differ. |
| 9 | Vote-weighted party gaps | Vote weighting barely changes the main gaps: welfare is +8.7 points unweighted and +7.9 weighted. | Methodological follow-up. |
| 10 | Topic attention and CF-scores | Pooled correlations follow the expected partisan ordering, but within-party correlations are weak. | Honest external validation; not a lead figure. |
| 11 | Collection depth by chamber | Median valid pages range from 31–33 for House candidates to 60–76 for Senate candidates. | Documents the archive; limited substantive appeal. |

## Public carousel

Use four panels:

1. Long-run partisan issue gaps.
2. Coverage against the official ballot.
3. The 2024 agenda comparison.
4. Document-length continuity at the 2016–2018 boundary.

The first and third figures show what the data can reveal. The second states
who is represented. The fourth checks whether document length remains
comparable when the source dataset changes.

Upload the four numbered PNG files in `release_carousel` in this order.

## Interpretation limits

- Keep the 2016–2018 source break visible in every combined series.
- Do not interpret topic attention as support for a policy position.
- Do not headline annual Senate trends; several party-year cells contain only
  22–34 candidates.
- Do not use cross-dataset trends for topics averaging less than about 1%.
- Describe changes over time as descriptive. The figures do not identify what
  caused party agendas to change.

## Files

The `figures` directory contains PNG and PDF versions of the seven rendered
candidates. The `data` directory contains the plotted values and sample counts.
Rebuild the data with the project interpreter:

```bash
.venv/bin/python scripts/social_figure_data.py \
  --release-dir build/release_candidate \
  --icpsr-dir /path/to/226001-V1 \
  --out-dir quality_reports/social_figure_candidates/data
Rscript scripts/social_figure_candidates.R
```
