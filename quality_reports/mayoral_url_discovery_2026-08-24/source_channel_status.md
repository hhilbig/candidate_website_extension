# Mayoral URL source channel status

## Current verdict

Archive-It is implemented and has completed its first live validation. It
provides public seed metadata without a paid key. Two collections contain
mayoral campaign URLs that the current legacy corpus does not cover.

The first expansion batch promotes three additional collections. Hanno accepted
all five grade-A custom-domain claims. Three increase candidate-year coverage;
two duplicate candidate-years already covered by the legacy baseline.

Commercial civic providers can help after 2018. They do not provide a verified
national route for 2000-2017. The project will request historical samples before
it considers a license.

The active acquisition frame now begins in 2010. Pre-2010 domains remain useful
as leads, but sparse Wayback coverage often prevents them from reaching grade A
or B. The project will retain existing pre-2010 evidence and pause new discovery
or records requests for that period.

## What the source test compares

The test compares unresolved ALGEL mayoral candidate-years with candidate and
campaign URLs in historical source records. A record counts only when its
candidate, city, office, and election cycle match.

## Archive-It

The Archive-It Partner API exposes collection and seed metadata without
authentication. Collection-specific CDX/C queries also have public access.

- UCLA collection `5903` contains 2,556 seeds. Fifty-five seeds have mayoral
  metadata. The collection covers Los Angeles cycles from 2001 through 2017.
- Birmingham collection `16766` contains six explicit 2021 campaign domains.
  The current roster has eight Birmingham candidates in 2021 and no accepted URL.
- Birmingham collection `9517` yields the Bell and Woodfin campaign domains for
  the 2017 runoff.
- Grand Rapids collection `13056` yields Rosalynn Bliss's 2019 campaign domain.
- Cleveland collection `17435` yields the Kelley and Bibb campaign domains for
  2021.
- Community Webs libraries publish through Archive-It. The same adapter can
  ingest their public collections.

The adapter uses the Partner API and collection-specific timemaps. It does not
depend on the global CDX endpoint. The two fixtures contain 61 mayoral seed
records and yield 33 matched claims for 30 candidate-years. Fourteen custom
domains pass the grade-A checks; 19 claims remain grade C. Hanno accepted all
33 identity, timing, and URL matches. The 14 grade-A custom domains are strict
new candidate-years; the 19 grade-C claims do not count toward strict coverage.

The three promoted collections add five accepted grade-A claims, with no
validation request errors. Bell 2017, Woodfin 2017, and Bliss 2019 are new
coverage. Bibb 2021 and Kelley 2021 already had legacy source-backed URLs.

The three promoted collections add five candidate-year claims. All five pass
the automated grade-A content and timing checks, with no validation request
errors. They remain excluded from coverage until human adjudication.

The public collection-catalog enumerator scanned 14,150 collections with 297
deterministic local queries. It retained 753 high-recall collection leads, 751
beyond the two fixtures. A 753-row priority table preserves one record per
collection, the matched-query provenance, and the raw catalog metadata. The
lead set is intentionally broad and does not count as coverage. Selected
collections still need seed matching and temporal ownership validation.

Official documentation:

- [Archive-It Partner API](https://support.archive-it.org/hc/en-us/articles/360032747311-Access-your-account-with-the-Archive-It-Partner-API)
- [UCLA collection seed endpoint](https://partner.archive-it.org/api/seed?collection=5903&limit=-1)
- [Birmingham collection seed endpoint](https://partner.archive-it.org/api/seed?collection=16766&limit=100)

## Search pilot

The completed 120-case search pilot recovers three strict grade-A custom
domains. Raw strict yield is 2.5 percent; the six-cell post-stratified estimate
is 2.90 percent, or 136 projected additions among 4,699 unresolved cases. The
90 percent bootstrap interval spans 0.24 to 6.33 percent. Hanno accepted six of
ten proposed URL leads and rejected four as temporally mismatched, for 60
percent lead precision. All three negative audits remained negative. The pilot
does not justify spending the remaining Serper credits.

## Civic-data providers

BallotReady reports historical candidacy data for most positions from 2018.
Its data products can include campaign websites and social links. A test key
and production data require provider contact.

Ballotpedia supports structured candidate and election data from 2018. Campaign
website fields require a contact-information add-on. API and bulk access require
a sales-issued key.

Democracy Works supports past election records and mayoral candidates in large
cities. Its documented candidate contact object can contain a campaign website.
Its published election history begins in 2020.

Vote Smart documents campaign web addresses and local executive offices. Its
API requires a licensed key. Organizational use can require a fee.

VOTE411 began in 2006. DemocracyNet covered 2000-2004. No public historical
bulk export is documented. Archive recovery is therefore the first test for
these sources.

Official documentation:

- [BallotReady data products](https://organizations.ballotready.org/data-products)
- [Ballotpedia candidate dictionary](https://developer.ballotpedia.org/dictionaries-and-terms/data-dictionary-candidates)
- [Democracy Works Elections API](https://www.democracy.works/elections-api)
- [Vote Smart Address API](https://api.votesmart.org/docs/Address.html)
- [VOTE411 history](https://www.vote411.org/about)

## Sources not scheduled now

The Library of Congress election package covers federal and gubernatorial
campaigns. It does not provide systematic mayoral coverage. The project will
not build a Library of Congress adapter in this phase.

The project also defers DMOZ and Arquivo.pt. The current research did not show
enough proven mayoral coverage to schedule them before Archive-It.

## Implication

Archive-It is a high-precision but low-yield channel in the blinded screen. All
nine screened claims are accepted target URLs, but only five meet the strict
A/B evidence rule. The channel therefore enters `hold`: it is above the drop
threshold of three strict URLs and below the continuation threshold of ten.
The first municipal pass on the 20 early-period cities produced no URL claims.
Three pre-2010 records requests were approved and saved as Gmail drafts, then
paused before sending. The next screen will cover 2010-2021 only. Search will
not continue in this phase, and no public-record or provider message has been
sent.
