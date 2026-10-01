# Plan: Store the completed mayoral source expansion

**Date:** 2026-09-10
**Author:** Codex session
**Status:** done

## Current state

The finished source-expansion run output uses 7.7 GB on mac2. The run stopped before the full source queue was processed. The primary Mac has about 19 GB free, so a full local copy would use too much of the remaining disk space. The existing attached droplet volume has about 37 GB free.

This task stores one compressed, verified archive on the attached droplet volume. It copies only the small audit and review files to the primary Mac. It leaves the mac2 source files in place.

## Transfer

The source is the completed `production` directory under the `mayoral_sources_20260908` run on mac2. The destination is a new backup directory under `/mnt/mayoral_wayback_full_snapshots` on the droplet.

The archive stream runs directly from mac2 to the droplet over Tailscale. It does not pass through the primary Mac. A SHA-256 hash of the uncompressed tar stream verifies that the destination archive contains the exact transferred bytes. The destination also receives an archive hash and a small transfer manifest.

## Local review bundle

The local repository receives the final audit files, job state, notification record, candidate URL outputs, and candidate roster. These files are sufficient to review the run without copying raw responses, parsed pages, text, or the 674 MB SQLite database.

## Verification

- Confirm that no source-expansion process is still running.
- Run SQLite integrity checks on the completed source database.
- Confirm the source file count and byte count.
- Test the compressed archive.
- Compare the source tar-stream hash with the decompressed archive-stream hash.
- Compare the archive member count with a fresh member count from the source tree.
- Hash the archive and record its exact size and storage path.
- Compare hashes for every file in the local review bundle against mac2.
- Measure free space on both Macs after the transfer.

## Limits

This task does not delete the mac2 source. It does not overwrite the existing local partial run. It does not resume the incomplete source pass.

The user separately approved deletion after the archive passed all verification checks. The mac2 source was deleted on 2026-09-10.
