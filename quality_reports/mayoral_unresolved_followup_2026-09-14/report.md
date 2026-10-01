# Follow-up for unresolved mayoral websites

The 47 unresolved proposals require three different types of work: archive inventory, date-specific evidence, and saved-content review.
The [follow-up table](followup.csv) preserves every unresolved proposal and identifies its next action.
It uses the existing validation findings and does not change candidate coverage.

Twenty-eight proposals have availability-negative results. These results do not establish archive absence; they require a bounded inventory proposal.
Eight have captures outside the 365-day window. One falls in the separate sensitivity window, while seven fall outside both windows.
The remaining 11 have saved captures whose campaign evidence was unresolved.

## AI review of the 11 saved-content cases

A GPT-5.6 Sol agent read the saved primary pages and four supplemental copies.
All 11 main hashes and all four supplement hashes matched their recorded values.
The coordinator verified every candidate key, candidate-year key, primary hash, and effective timestamp against the input table.
The [AI review](ai_content_review.json) records five support-with-caveat decisions, five rejections of the captured campaign evidence, and one unresolved case.

| Candidate | Review finding |
|---|---|
| Gary Woronchak | Campaign controls and a mayoral announcement support the Dearborn campaign; old state-senate metadata remains. |
| Lovely Warren | Campaign ownership is supported, but saved text omits the city and election year. |
| Lemichael Wilson | A campaign payment statement and a Memphis-mayor image filename support ownership; the entry page provides little content. |
| Erica Gilmore | Metadata names a mayoral campaign, but the client-rendered page lacks substantive city and election text. |
| Jose Solorio | Campaign title and payment statement support the 2020 Santa Ana campaign; prior domain-reuse flags require a separate check. |
| Tina Rane Alexander | Saved metadata describes professional services, without mayoral campaign evidence. |
| William Bell | The campaign page concerns Birmingham; the candidate record concerns Durham. |
| Sunshine Grund | The personal resume lists past campaigns but does not establish a campaign page. |
| Sean Davis | The page promotes his writing and professional work, without mayoral campaign evidence. |
| Dennis Herrera | The saved capture contains generic insurance advice, not the named mayoral campaign. |
| Julia Clark-Johnson | The client-rendered page provides insufficient campaign, office, or city evidence. |

These are capture-specific findings. They do not establish the content of every earlier or later page on the same domain.
The five caveated cases need the specified remaining checks before full validation.
The five rejected captures cannot supply campaign evidence for the current candidate-year records.
All source proposals and previous validation fields remain preserved.

The initial review header mistakenly counted five supplements. The coordinator found four provenance entries, and the reviewer corrected the count.
No main hash, review decision, or candidate count changed in that correction.

The coordinator also checked Solorio's apparent name mismatch against the saved body and parser code.
Accent normalization already works. The saved visible text omits Jose and supplies only the surname.
The review explanation now identifies the missing first name, rather than suggesting an accent bug.
A regression test confirms that accented full names match while surname-only campaign titles remain insufficient for the full-name check.

The follow-up script passed three tests and compiled successfully.
This pass used saved files and session agents. It made no new archive requests and copied no raw pages locally.
