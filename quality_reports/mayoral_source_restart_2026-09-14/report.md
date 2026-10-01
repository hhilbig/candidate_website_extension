# Mayoral source discovery resumed

The approved tranche runs on the existing droplet data volume.
It compares candidate URL leads before and after more source-page attempts, starting from 1,093 candidate-years with leads among 2,788 active cases.
It does not repeat the completed campaign-snapshot scrape.

## Approval and limits

Hanno approved at most 5,000 additional source attempts and 4,000,000,000 new raw response bytes.
The implementation counts decoded response bodies, not network headers or TCP traffic.
The persistent tranche ID is `source-discovery-20260914`.
Restart with this same ID and the same limits. Do not create another ID to bypass a stop.

Each dispatch reserves 25,065,536 bytes before its request starts.
A completed result releases unused bytes. Interrupted and uncertain transport-error attempts retain their full reservation.
The runner can therefore stop below the byte ceiling. The attempt count also includes reserved but interrupted work.
No human acceptance or campaign ownership rule changed.

The restored source ledger contains 58,463 sources, 85,049 candidate-source relations, and 14,042 prior attempts.
The source queue previously retained 43,961 never-attempted sources and 415 retryable sources.
At most 5,000 new attempts cannot exhaust that queue. All remaining sources stay preserved.
The runner uses two workers on this smaller host and keeps at least 8 GB of free space.
No paid search, new storage, or data purchase occurred.

## Restore and runtime checks

Both archive SHA-256 hashes match their saved manifests.
The restored database passed its integrity check and the expected table counts.
All 40 manifest-listed file hashes passed before code deployment.
The coordinator verified 9,828 raw, 9,647 parsed, and 9,545 text file hashes referenced by current records.
Both archives and the original restored code remain preserved.

The original parser and ownership-rule hashes match the corrected September 10 archive.
The deployment changes only the source limit controls and the supervisor.
Portable Poppler and Tesseract tools reside under the deployment directory, not the system installation.
The runtime also uses project-local pypdf and test dependencies.
All 113 relevant tests passed locally and on the droplet, including PDF annotations and actual OCR.
The shell launch command and Python files passed syntax checks.

The [launch contract](launch_contract.json) freezes the code hashes and approved limits.
The [restore verification](restore_verification.json) records baseline counts and hashes.
The [launch verification](launch_verification.json) records the first successful saved result and active process checks.

## Live state and notifications

The supervisor started at 18:05:45 UTC on September 14.
At 18:06:43 UTC, one new source had completed and another had started.
The first raw response passed its SHA-256 check; parsed and text files also exist.
Both worker and supervisor processes were active with the correct tranche arguments.
The data volume retained about 24.6 GB free at this check.
Do not estimate final yield or ETA from this first source.

The supervisor invokes the existing Slack helper.
Messages identify Mayoral URL discovery and candidate_website_extension.
It sends start, six-hour progress, stall, and completion or failure messages.
The start helper invocation succeeded; the helper does not provide a delivery receipt.

The authoritative remote run directory is:

```text
/mnt/mayoral_wayback_full_snapshots/source_discovery_20260914/work/production
```

Read its `sources.sqlite`, `progress.json`, `job_state.json`, and `source_pass.log` for live state.
Read `combined/` for the latest completed export, which can lag database progress.
The supervisor log is `supervisor.log` in the parent deployment directory.
No local baseline CSV was replaced by the new run.

## Restart and next decision

If interrupted, confirm that no child or supervisor remains before restarting.
Then run this command detached on the droplet with the same limits:

```bash
bash /mnt/mayoral_wayback_full_snapshots/source_discovery_20260914/code/scripts/launch_mayoral_source_tranche.sh \
  /mnt/mayoral_wayback_full_snapshots/source_discovery_20260914 \
  /root/candidate_website_extension/.venv/bin/python \
  /root/candidate_website_extension/scripts/slack_status.sh
```

After the tranche stops, compare its new candidate-year leads with the frozen 1,093 baseline.
Review new ownership evidence and report source failures separately.
Another tranche requires a new approval.
