# Historical civic-provider feasibility, 2010–2021

BallotReady/CivicEngine is the first provider to test because it explicitly advertises campaign websites and candidacy results since 2018.
Ballotpedia is the second test because its candidate data are organized by election, stage, and candidate, but campaign websites are a paid contact add-on.
Democracy Works is a conditional third test for 2020–2021 because its candidate website data are sourced from Ballotpedia and public documentation does not establish access to historical versions.
The public evidence does not justify a purchase: no reviewed provider publishes both a usable price and terms that clearly allow retention and release of accepted historical URLs.
No provider was contacted, no form was submitted, no API key was requested, and no paid service was used.

## Question and comparison

This workstream asks whether a civic-data provider can supply candidate-owned campaign website URLs tied to the exact mayoral election and round in which the provider observed them. I compared the same unresolved candidate-years across provider coverage dates, historical data structure, observation-date support, delivery method, license constraints, and public cost information.

The result is a feasibility screen, not evidence that any provider has recovered a URL. A provider value can enter the project only after it is matched to the requested candidacy and passes the existing archive-based temporal ownership validation.

## Assumptions

1. An unresolved candidate-year is a row in the frozen mayoral summary with no validated campaign URL.
2. The 2010–2021 frame is frozen from `data/municipal_discovery/screening_2010_2021/search_source_expansion/frozen_inputs/unresolved_2010_2021.csv`; its exact input and output hashes are in `sample_manifest.json`. The parent directory now contains the post-search coverage frame and must not replace this input.
3. The primary provider pilot covers 2018–2021 because BallotReady advertises candidacy results and campaign websites since 2018 and Ballotpedia documents election-date data from 2018 onward. Democracy Works receives only the 2020–2021 subset.
4. “Masked” means that internal identifiers, known URL outcomes, and discovery status are withheld. Candidate name, city, state, office, and date context remain because a provider cannot match a candidacy without them.
5. Each provider must return one terminal row per requested case, including `no_result`, `match_unresolved`, `source_unavailable`, or `request_error` when it cannot return an event-specific URL.
6. Current-profile values and undated historical values are recorded for audit but do not qualify as event-specific evidence.

## Frozen frame and deterministic sample

The current unresolved 2010–2021 frame contains 2,669 candidate-years and no covered rows. The common acquisition screen is a 480-case subset of this frame, with 160 cases in each of three periods. The primary provider era, 2018–2021, contains 889 unresolved candidate-years in the frame and 160 cases in the common screen. The provider pilot draws 25 cases from each year within those 160 cases using SHA-256 ranking and seed `20260825`, for 100 cases total. Democracy Works is requested only for the 50 cases from 2020–2021.

| Year | Unresolved frame | Common-screen cases | Provider cases | Conditional inclusion probability |
|---:|---:|---:|---:|---:|
| 2018 | 259 | 34 | 25 | 0.7353 |
| 2019 | 242 | 43 | 25 | 0.5814 |
| 2020 | 212 | 42 | 25 | 0.5952 |
| 2021 | 176 | 41 | 25 | 0.6098 |

The packet spans 31 states. It has exact day information for 23 cases and month information for 77. This is why the requested provider response must return the exact election date and round rather than simply echo the request packet. Each row’s combined inclusion probability is its probability in the common screen multiplied by the conditional probability in the last column; the crosswalk retains both components and the resulting weight.

Three reductions exceed 20% and are intentional. The inherited common acquisition screen contains 480 of the 2,669 unresolved cases and leaves 2,189 outside this screening sample (82.0%). Restricting that common screen to 2018–2021 retains 160 of 480 and leaves 320 cases from earlier periods outside the provider test (66.7%). The deterministic provider pilot retains 100 of those 160 cases and leaves 60 outside the provider subsample (37.5%). None of these operations deletes records from the frozen frame; every reduction and its reason is recorded in the manifest.

The manifest pins the unresolved-frame SHA-256 hash as `f2c8c6f95f471391fac223068c8e4d9dd58c950cd092e50f64a83aef8f871ed5` and the common-screen parent hash as `646e2688005ca7c4f0f20ed027aeb7709351adbdfa9864af26e143f1caf3eb19`. The build fails if either the frame or parent includes a covered row, if a parent identifier is outside the unresolved frame, or if shared identity fields disagree.

Artifacts:

- `data/frozen_unresolved_2010_2021.csv`: internal frozen frame, not a provider attachment.
- `data/masked_provider_sample_100.csv`: the provider-facing packet.
- `data/internal_masked_sample_crosswalk.csv`: internal masked-ID crosswalk and sampling weights, not a provider attachment.
- `data/provider_response_template.csv`: the required return columns.
- `config/provider_response_schema.json`: validation and event-specific eligibility rules.

## Provider findings

### 1. BallotReady/CivicEngine

BallotReady is the most direct first test. Its [Data Products page](https://organizations.ballotready.org/data-products) says the Candidacies Dataset includes campaign websites, social links, and win/loss results since 2018 for most positions. It offers API access and customizable data packages in CSV or Avro. Its [data-tier documentation](https://support.ballotready.org/article/733-ballotready-data-tiers) places municipal candidates in cities above 50,000 population in the default tier, which fits this large-city project better than a general local-election sample. The [demo page](https://organizations.ballotready.org/data-products-request-a-demo) offers a free sample and pricing information after contact.

The decisive unknown is historical versioning. The reviewed public pages do not say whether a campaign website remains tied to the 2018, 2019, 2020, or 2021 candidacy, whether the provider records when it observed the URL, or whether a current value overwrites the historical one. They also do not publish a data license or price schedule. Proceed only with an approved no-cost sample request that asks these questions and obtains the proposed terms.

### 2. Ballotpedia

Ballotpedia has the strongest public event schema. Its [candidate data dictionary](https://developer.ballotpedia.org/dictionaries-and-terms/data-dictionary-candidates) contains election date, stage, race, office, district, and candidate identifiers. Its [candidate-data explanation](https://developer.ballotpedia.org/dictionaries-and-terms/about-the-candidates-data-set) says a row represents a candidate’s participation in one stage, or a candidate-stage-round in ranked-choice voting. Its [election-date endpoint](https://developer.ballotpedia.org/geographic-apis/election_dates) documents data from 2018 onward, and [elections by state](https://developer.ballotpedia.org/geographic-apis/elections_by_state) supports past election dates.

The campaign website field is part of an additional-charge contact add-on, not the base candidate file. Public [client terms](https://developer.ballotpedia.org/dictionaries-and-terms/terms-of-use) treat licensed data as proprietary and confidential, prohibit sharing full data sets, and require protection against third-party bulk downloads. Those terms do not resolve whether accepted individual campaign URLs and provenance may be released in a research dataset. Request the same 100 cases, observation dates, a quote, and the complete proposed license before considering purchase.

### 3. Democracy Works

Democracy Works is relevant only for the 50 cases from 2020–2021. Its [Elections API page](https://www.democracy.works/elections-api) reports more than 14,000 federal, state, and local elections published since 2020. Its [API 2.0 documentation](https://developers.democracy.works/api/v2) includes a campaign website in candidate contact records and documents mayoral candidates in large cities. It requires an API key and directs prospective users to contact the provider for pricing and a test key.

The channel is not yet an independent source. The same documentation says ballot data are sourced from Ballotpedia, and its API introduction emphasizes upcoming elections. The public pages do not establish that 2020–2021 campaign website versions remain accessible or dated. Test it only if the provider confirms a historical event-specific export and explains whether its URLs differ from the Ballotpedia records being tested directly.

### 4. Vote Smart and LWV/VOTE411

Vote Smart is technically plausible but secondary. Its [Address API](https://api.votesmart.org/docs/Address.html) exposes a campaign web-address method, including candidate identifiers and election-address methods. Its [API terms](https://api.votesmart.org/docs/terms.html) make organizational use fee-based, prohibit key sharing, and allow access revocation with removal of Vote Smart content. The public documentation does not establish historical local-mayoral completeness, URL observation dates, or research release rights.

VOTE411 is a permission-based archival lead, not a scraping target. The League’s [2019 administrator guide](https://www.lwv.org/sites/default/files/2019-04/VOTE411HowTo2019.pdf) documents candidate-data exports and post-election archiving. Current [VOTE411 legal terms](https://www.vote411.org/legal) prohibit automated queries without advance permission and restrict organizational redistribution without written consent. Any test must ask the League or a local League for an existing historical export and written research-use permission.

## Terms, cost, and license gate

No public source supplies enough information to calculate cost per accepted URL. BallotReady offers a free sample but no public price; Ballotpedia labels campaign contacts an additional-charge field; Democracy Works and Vote Smart provide pricing after contact; VOTE411 does not advertise a historical-data product. The cost numerator must include setup fees, minimum contract charges, subscription charges required to receive the export, and analyst integration time. The denominator must include only URLs that ultimately receive strict evidence grade A or B.

Before any purchase, the returned sample and proposed agreement must answer every required item in `config/terms_cost_license_checklist.json`. The channel continues only if adjudicated precision is at least 95% and projected cost is below $100 per accepted strict URL. A channel is dropped if precision falls below 90% or projected cost exceeds $250 per accepted strict URL. Hanno must approve the sample, quote, terms, and projected cost before any paid access.

## Proposed next decision

After exact recipient and message approval, send the same 100-case packet and response schema first to BallotReady/CivicEngine, then to Ballotpedia. Ask for a free feasibility response, full terms, and a production quote without agreeing to service. Democracy Works should receive its 50 eligible cases only after the first two responses, unless Hanno prefers a simultaneous no-cost comparison. Hold Vote Smart and VOTE411 until the first three channels fail their yield, precision, or cost gates.

The provider path cannot resolve the whole 2010–2021 gap. Even a successful primary-provider test directly targets only 889 of the 2,669 unresolved cases in this frame. The 1,780 unresolved cases from 2010–2017 remain assigned to archive and municipal-record channels.

## Source and access record

All claims above use public primary provider pages accessed on 2026-08-25. Exact URLs and the claim supported by each page are recorded in `config/provider_source_inventory.json`. No provider account, authenticated endpoint, form submission, or paid access was used.

## Glossary

**Candidate-year:** One person’s candidacy in one election year.

**Event-specific:** Tied to the candidate, office, jurisdiction, election date, and round that produced the campaign URL.

**Observation date:** The date the provider recorded the URL or the dated historical version from which the value came.

**Strict URL:** A candidate-owned campaign URL that later passes the project’s archive-based grade A or B evidence rule.

**Masked sample:** A provider packet that hides internal identifiers and known URL outcomes while retaining the identifying election context required for matching.
