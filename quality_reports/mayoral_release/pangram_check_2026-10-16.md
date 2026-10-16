# Pangram check of the mayoral release documents: October 16, 2026

Pangram classified most of both documents as AI-written. These are detector classifications, not facts about authorship.

Hanno asked for the check. Results were read in the signed-in Pangram dashboard (Pangram 4.0, 1 credit per 100 words). Two checks used 26 credits; the balance went from 1,531 to 1,505.

Input: `docs/mayoral_release/README.md` and `CODEBOOK.md` at commit `06c5a19`, converted to plain text without changes to the wording. The README code blocks were removed. Table rows were flattened to "label: text" lines.

| Document | Pangram words | Result | Segments |
|---|---:|---|---|
| README | 1,398 | 80% AI, 20% human | Human: the opening through the file table (221 words) and the terms of use (34 words). AI: population, collection, checks, coverage and limitations (1,103 words) |
| Codebook | 1,180 | 88% AI, 12% human | AI: keys through `mayoral_finalists.csv` (631 words) and the page corpus and panel sections (406 words). Human: 50 words around the `url_lead_basis` note |
