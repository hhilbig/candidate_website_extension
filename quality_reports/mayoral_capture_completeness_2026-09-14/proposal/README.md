# Proposed exact-URL acquisition

Status update: the user approved this batch on September 14. All 259 captures are now saved and independently verified on the droplet, within the approved byte limit. See [acquisition verification](acquisition_verification.json). The text below records the original proposal; its not-launched status is historical. No new inventory or deferred-key acquisition was authorized.

The proposal covers 259 never-queued captures at exact AI-accepted URLs across five candidate-years.
It compares the known inventory gaps with the requested original URLs and retains each recorded capture timestamp.
The [queue](exact_capture_queue.csv) gives stable request keys and replay URLs.
The [manifest](manifest.json) records input hashes, scope, estimates, and proposed limits.
This proposal has not launched.

The observed mean response size implies about 12.49 MB of raw responses or 2.65 MB compressed.
This estimate excludes text, metadata, and retry responses. It is not an upper bound.
The proposed total ceiling is 128 MiB of decoded responses, with at most 2 MiB per response and three attempts per capture.
The runner must enforce these limits persistently across interruptions and restarts.
It must use one worker and retain at least 8 GB free on the attached volume.
All raw pages and extracted text would stay on that volume.

This first proposal selects 259 of 2,233 known unqueued keys and defers 1,974, or 88.4 percent.
The deferred keys contain 69 URL variants and 1,905 other paths or hosts that still require target review.
They remain in the upstream audit queue and remain part of the outstanding work.
The proposal does not replace the complete collection objective with the exact-URL subset.

New inventories for 22 URLs remain separate. Their capture counts and byte requirements are unknown.
The user must resolve whether the next inventory retains every timestamp or distinct page content.
Metadata queries can proceed only under explicit bounds that preserve incomplete results and continuation state.

Before any launch, approve the acquisition scope, verify saved keys again, and implement the manifest's byte controls.
The archive-only redirect policy must retain changed targets and timestamp substitutions as unresolved evidence.
The proposed file-byte ceiling does not guarantee completion if archive responses exceed it.

The proposal builder and related audit checks passed 14 tests. The builder compiled successfully.
