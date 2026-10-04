# Proposal: resize the droplet volume from 50 to 100 GiB

Written September 16, 2026. This proposal asks for one paid change. Nothing is resized until Hanno approves.

## Why this is needed

The 2026 collection is blocked. Its storage gate fails.

| Item | Value |
|---|---:|
| Base forecast for the remaining collection | 27.78 GiB |
| Total including the approved 2x planning allowance | 55.56 GiB |
| Headroom the gate requires | 5.00 GiB |
| Total the gate needs | 60.56 GiB |
| Free space on the volume today | 19.32 GiB |
| Shortfall | about 41 GiB |

`scripts/collect_2026.py enable-full` refuses to start full collection while `storage_fits` is false.
The forecast is `data/collection_2026/monthly_cadence_20260915/pilot_forecast.json`.

**This track has a deadline and its data cannot be recreated.** Collection ends December 26, 2026, which is
the last relevant event plus 14 days, extended by December runoffs. A live copy not taken in October is gone.
The Wayback comparison windows can be revisited later; the live copies cannot.

**One volume serves two tracks.** The mayoral acquisition queues and the 2026 collection share `/dev/sdb`.
Mayoral step 3 still holds 44,883 unattempted sources, 637 retryable sources, and 7,380 pages awaiting
optical character recognition. Both tracks compete for the same 19.32 GiB, so one resize unblocks both.

## Why free space alone does not solve it

The volume holds 28 GiB. The largest directories are `source_discovery_20260914` at 10 GiB, `runs` at
8.1 GiB, and `backups` at 8.0 GiB. Deleting every mayoral directory would leave about 48 GiB free, which is
still below the 60.56 GiB the gate requires, and it would destroy evidence this project still needs.

## The exact change

| Item | Value |
|---|---|
| DigitalOcean volume name | `mayoral-wayback-full-snapshots` |
| Region | `sfo2` |
| Droplet id | `548757703` |
| Device and filesystem | `/dev/sdb`, ext4 |
| Mount | `/mnt/mayoral_wayback_full_snapshots`, systemd unit `mnt-mayoral_wayback_full_snapshots.mount` |
| Current size | 50 GiB |
| Proposed size | 100 GiB |

`doctl` is not installed locally or on the droplet, so the resize itself is a control-panel action.

## Cost

DigitalOcean block storage is $0.10 per GiB per month. That rate is the one this repository's earlier
storage proposals used, where 400 GiB was quoted at $40 per month.

| Item | Amount |
|---|---:|
| Current 50 GiB | $5.00 per month |
| Proposed 100 GiB | $10.00 per month |
| Increase | $5.00 per month |
| Increase through December 26, 2026, about 3.3 months | about $17 |
| Increase if the volume is kept a full year | $60 |

The volume can be reduced again only by creating a new smaller volume and copying data, so treat this as a
decision to hold 100 GiB until the mayoral data is archived elsewhere.

## Steps

1. **Hanno resizes the volume.** In the DigitalOcean control panel, open Volumes, select
   `mayoral-wayback-full-snapshots`, choose Increase Size, and set 100 GiB. DigitalOcean grows a volume
   online and never shrinks it, so this does not interrupt the droplet.
2. **I grow the filesystem.** On the droplet, `sudo resize2fs /dev/sdb`. Growing a mounted ext4 filesystem
   online is supported, so no unmount and no downtime are needed. I confirm with `df -h /mnt/mayoral_wayback_full_snapshots`.
3. **I re-run the forecast.** `python scripts/collect_2026.py forecast` and confirm it now reports
   `storage_fits: true`.
4. **I enable full mode.** `python scripts/collect_2026.py enable-full`, which writes `full_enabled.json`
   only when the gate passes.
5. **The existing automation resumes collection.** The Codex heartbeat `collect-2026-campaign-websites`
   owns this track. I create no scheduler.

## Risks

Growing the filesystem is non-destructive and reversible in the sense that nothing is written or deleted.
The real risk is the opposite one: leaving the gate closed loses live copies permanently, one day at a time.

Root is a separate concern. `/dev/vda1` is 24 GiB with 3.1 GiB free at 87 percent. This proposal does not
change root. If root fills, the collection fails for a different reason, so it is worth watching separately.
