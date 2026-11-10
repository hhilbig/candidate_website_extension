# Draft for version 5.0: README text for the 2026 data

Status: draft for Hanno's approval. All counts are from the corpus of November 10, 2026.
The archive pass after December 3 changes them; the build fills the final counts.
The draft assumes that the 2026 rows go into the existing files, with the new columns empty for earlier years.

## New section: 2026 collection

The 2026 rows differ from the earlier years in three ways.

**Source.** For 2002–2024, all text comes from the Internet Archive. For 2026, the
project also copied each campaign website directly, from September 14, 2026 through
November 5, 2026. Archived copies of 2026 start in January 2026. `capture_source` is `live` for a direct copy and `wayback` for an archived copy.
`snap_url` is missing for a live row, because no archived copy exists for it.

**Frequency.** Each 2026 website has a direct copy about every 30 days, and a copy on
the day before and the day after each primary or runoff of the candidate's state. A
candidate therefore has several dated copies (median 6). `snapshot_id` identifies one
copy of one website; `date` is the time of each page. `panel_snapshot_day.parquet`
holds the measures for each copy day. `panel_candidate_year.csv` keeps the copy with
the most text, as in the earlier years; `sel_snapshot_id` names that copy.

**Search record.** For 2026, the project searched for a website for each candidate and
recorded each failed copy. The provenance file therefore uses three status values that
do not occur before 2026: `no_url_found`, `valid_url_no_usable_capture` (with the cause
in `collection_failure_reason`), and `captured_under_other_fec_id`.

The 2026 population has 3,656 candidate-years. 2,459 have a website URL, and 1,735 have
usable text (105,020 page rows). Of the 954 general-election candidates, 706 have usable
text.

## Changes to existing sections

**Population.** Add House and Senate records of 2026. Five general-election candidates
whose FEC record is outside the population rule enter as reviewed additions. Fifteen
persons have two FEC IDs in 2026; the text is under one ID, and the other ID has
`capture_status = captured_under_other_fec_id`.

**`on_ballot` in 2026.** Official returns for 2026 are not yet published. `on_ballot`
comes from the general-election candidate lists of the English Wikipedia state pages
(the release lists each page and its revision ID). `general_votes` is missing for 2026. A later
version replaces this source with official returns. For 2026, `data_source` is
`general_2026_collection` or `election_year_2026_collection`.

**Page types in 2026.** `page_type` follows the same URL rule as the earlier years.
`page_type_context` adds the type that the link text implies (a page reached from the
menu entry "Issues" is `issues`). The extracted issue and biography records use both;
`extraction_route` shows the route of each page. Endorsement records use the URL rule
only.

**Identity.** A 2026 page is in the corpus only if its text names the candidate
(family name and given name, or a listed short form of the given name).

**Limitations.** Add: (0) the 2026 corpus omits 242 archived pages whose stored body was compressed data, not text; (1) direct copies can fail when a website blocks automatic
requests; `collection_failure_reason` records this; (2) the validation of the extracted
2026 records is model-coded, as in version 4.0; (3) `bonica_rid` in 2026 is carried
from an earlier year of the same FEC ID and is missing for new candidates.

## Terms of use: added paragraph (for approval)

> The 2026 rows with `capture_source = live` are copies that the project made from
> public campaign websites; `urlkey` gives the address of each page and `date` the time
> of the copy. No archived URL exists for these rows. Copyright in this material remains
> with its authors, and the same removal procedure applies.

## Open points for Hanno

0. The release does not yet hold the list of Wikipedia pages and revision IDs (now in `data/collection_2026/ballot_2026/pages.json`). I add it as a small file if you approve the sentence above.
1. Title and first paragraph: "House (2018–2026) and Senate (2002–2026)".
2. Whether the Wikipedia source for `on_ballot` needs a stronger caution than the text above.
3. The contact address for removal requests is still open from version 4.0.
