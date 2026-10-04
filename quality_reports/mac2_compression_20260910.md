# mac2 compression, September 10, 2026

Two historical SQLite snapshots in `local-party-websites-germany` were replaced with lossless gzip archives after Hanno approved compression with all content preserved. The comparison is each original database's complete byte stream versus its archive's complete decompressed byte stream.

Both comparisons passed SHA-256 and byte-count checks. Both databases passed SQLite `PRAGMA quick_check` before replacement. The archives preserve every database byte; each has an adjacent `.manifest.json` containing checksums, original metadata, and restoration commands. SQLite sidecars remain in place.

## Storage result

The two original files totaled 26,837,024,768 bytes; the archives total 4,462,544,798 bytes. Their logical size fell by 20.84 GiB. The repository's logical total fell from about 62.1 GiB to 41.2 GiB.

**This did not free physical disk space.** mac2's available space fell from 13,748,076 KiB (13.11 GiB) to 9,613,720 KiB (9.17 GiB). The earlier estimated free-space gain was wrong because it treated logical file sizes as independently allocated storage. Shared APFS blocks with remaining database copies are a plausible explanation, but block sharing was not directly verified. The data volume reports no snapshots, and the open-unlinked-file check showed no retained database handles. Further compression should account for physical allocation before proceeding.

## Files and restoration

Run these commands on mac2 when the original database files are needed. Each restoration needs about 12.5 GiB of additional free space, which exceeds current availability. `-k` retains the archive.

```sh
gzip -dk -- /Users/hannohilbig/local-party-websites-germany/data/staging/recovery_20260715/party_scraper_pre_recovery_baseline.db.gz
gzip -dk -- /Users/hannohilbig/local-party-websites-germany/data/staging/source_recall_audit_v1/party_scraper_audit.db.gz
```

Both restored databases must have SHA-256:

```text
91eadf6ba2069d6fe06514600f8f597bcf7053028243430b4b565ee33be8deee
```

The full files were decompressed to a streaming checksum, rather than restored to extra full-size files on mac2. A small SQLite fixture verified restoration to disk, exact bytes, and readable rows. The test also covered corrupt archives, existing destinations, nonempty write-ahead logs, and resuming a partial archive. The initial run stopped safely because the validation connection remained open; explicitly closing that connection fixed the issue. All checks then passed.

The active database and staged database remain uncompressed. No scraper was restarted, and no acquisition ceiling was changed. Full research pipelines were not rerun: the changed files are historical snapshots that require restoration before use.
