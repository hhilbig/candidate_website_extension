# Session log: Mayoral source-expansion storage

**Date:** 2026-09-10
**Status:** done

## Result

The finished 7.7 GB source-expansion run directory is stored as one verified 6.36 GB archive on the attached droplet volume. The primary Mac received a review bundle that uses 8.4 MB on disk. After separate user approval, the full source was deleted from mac2 and its free space increased from 11 GB to 19 GB.

## Checks

The source job reports `finished` with exit code 0, and its worker and supervisor processes are absent. SQLite returned `ok` from `PRAGMA integrity_check`. The source contains 29,890 files totaling 8,164,812,284 bytes.

The destination archive passed `zstd -t`. Its decompressed tar stream has the same SHA-256 hash as a fresh tar stream on mac2: `fe736c6d5f497bf1076fa2db5e0c3384c5ecb05648ff0f9d9a1c1309f53ac850`. Both contain 29,896 members. The compressed archive is 6,363,710,956 bytes and has SHA-256 `78cc16b991f58eab9aa16369726c9b6294e1d1f72c996a60b41ac90fc6af935e`.

All nine review-bundle files match their mac2 SHA-256 hashes. The transfer manifest records each hash and both storage paths.

## Transfer issue and fix

The first checksum side pipe failed because the local sandbox blocked `/dev/fd/14`. A second route through the primary Mac was valid but slow. Both incomplete files, 35 MB and about 40 MB, were removed before replacement. The final transfer ran directly from mac2 to the droplet over Tailscale and completed successfully.

## Next action

The source pass processed 11,900 of 58,463 source rows. It left 43,961 rows unattempted and 415 rows retryable. Do not resume it on mac2 while only 11 GB is free.

No immediate user action is required. A later resume should restore the archive to storage with enough room for the remaining queue.
