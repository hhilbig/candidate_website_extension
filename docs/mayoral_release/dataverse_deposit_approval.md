# Mayoral dataset: Dataverse deposit for approval

Please mark each item **OK** or write the change you want in the Comment line. I will not create anything on Dataverse until you return this file.

The deposit is a new dataset in the root Harvard Dataverse collection, which also holds the House/Senate dataset (`doi:10.7910/DVN/BZ2JRS`). Its metadata fields follow the House/Senate deposit unless noted.

## 1. Title

Proposed: **Campaign Websites of Mayoral Candidates in the 100 Largest U.S. Cities, 2010–2026**

Alternative: *Archived Campaign Websites of U.S. Mayoral Finalists, 100 Largest Cities, 2010–2026*

The proposed title matches the README heading except that it says "candidates" instead of "finalists". The data also include primary candidates.

Comment: Maybe catchier name (acronym like CLARA)? Also I want to extend to more than top 100 so we have to remove this 

## 2. Author

Hilbig, Hanno. Affiliation: University of California, Davis (ROR `https://ror.org/05rrcem69`). This is the same as the House/Senate deposit.

Comment: yep

## 3. Contact email

The House/Senate deposit uses **hannohilbig@gmail.com** as the contact. The README takedown clause uses **hhilbig@ucdavis.edu**. Dataverse forwards contact-form messages to this address, but it does not show the address publicly.

Proposed: hhilbig@ucdavis.edu, so that the contact and the takedown clause use the same address.

Comment: yepo the davis one 

## 4. Description

Proposed text:

> Archived campaign websites of mayoral candidates in the 100 largest U.S. cities, January 2010 to September 2026. For each election that decided who became mayor, the data include the winner and the runner-up; in cities with partisan mayoral primaries, they also include the top two candidates of the decisive primary. All pages come from the Internet Archive's Wayback Machine. The release contains a corpus of 46,466 archived pages with flags for usability and campaign period, candidate-level panels with the joined website text, files on cities and elections, and a crosswalk to the American Local Government Elections Database (ALGED). Of the 860 finalists in 430 elections, 710 have usable website text. See README.md for the population and the collection, and CODEBOOK.md for every column.

Comment: Ok 

## 5. Subject and keywords

- Subject: Social Sciences (the same as House/Senate)
- Keywords: campaign websites; mayoral elections; local politics; Wayback Machine; text as data

The House/Senate deposit has only the keyword "campaign websites".

Comment: Ok

## 6. Time period covered

2010-01-01 to 2026-09-14, which is the window of the data. The elections in the data run from 2010-02-06 to 2026-07-21. The House/Senate deposit does not fill this field. I propose to fill it because it shows on the dataset page and in search results.

Comment: Ok

## 7. Related datasets

Proposed:

- de Benedictis-Kessner, Justin, Diana Da In Lee, Yamil R. Velez, and Christopher Warshaw. American Local Government Elections Database. OSF, https://osf.io/mv5e6/ (the crosswalk links to it, and its license is CC BY 4.0)
- Hilbig, Hanno. U.S. House (2018–2024) and Senate (2002–2024) Candidate Websites. Harvard Dataverse, https://doi.org/10.7910/DVN/BZ2JRS

You asked to remove the House reference from the README. This metadata field is separate from the README, and readers see it only on the dataset page. Keep or drop the second entry?

Comment: Ok 

## 8. License

CC BY 4.0, the same as House/Senate and as stated in the README. There are no custom terms of use, and the takedown clause stays in the README.

Comment: Ok

## 9. Files

All files are public and unrestricted. I will disable tabular ingest, as for House/Senate, so that the CSV files stay byte-identical and are not converted to `.tab`.

| File | Size |
|---|---:|
| README.md | 6 KB |
| CODEBOOK.md | 22 KB |
| manifest.json | 2 KB |
| mayoral_cities.csv | 25 KB |
| mayoral_city_years.csv | 0.9 MB |
| mayoral_elections.csv | 0.4 MB |
| mayoral_finalists.csv | 0.4 MB |
| mayoral_page_corpus.parquet | 41.7 MB |
| mayoral_finalist_panel.csv | 263.3 MB |
| mayoral_primary_elections.csv | 13 KB |
| mayoral_primary_candidates.csv | 43 KB |
| mayoral_primary_panel.csv | 13.7 MB |
| mayoral_ledb_crosswalk.csv | 0.1 MB |

The total is about 320 MB. The data files are those in `build/mayoral_release_v6_candidate/`, which passed the validator. README and CODEBOOK are the current versions in `docs/mayoral_release/` (README commit `6b8fcc2`).

Comment: Ok

## 10. Steps after your approval

1. Create the dataset as an unpublished draft with the metadata above. Dataverse reserves a DOI at this step, but the DOI does not resolve yet.
2. Upload the 13 files, then download the draft and check every file byte for byte against the local copies.
3. Report the draft link and the DOI to you, so that you can look at the draft page.
4. Publication is a separate step that needs your explicit approval. The API token cannot publish directly. For version 4.0 of the House/Senate dataset, the draft was submitted for review and Harvard curators published it. I will do the same only after you say so.

The README needs no change after publication, because its citation line points to the Dataverse page.

Comment: Ok

## Decisions (October 24)

- Title: **Archived Campaign Websites of U.S. Mayoral Candidates** (no acronym; no city count or years, so the title stays valid when the sample grows). The README heading now matches.
- Contact: hhilbig@ucdavis.edu.
- Items 2 and 4–10 approved as proposed, including both related datasets (ALGED and House/Senate).
