#!/usr/bin/env bash
# Merge an uncompressed candidate dir into an existing year tarball.
#
# Used for Senate 2004/2006/2008/2014, where a March tarball and an April
# re-scrape dir coexist. Extracts the tarball into scratch, folds in the
# dir's files (NEW files copied; on filename collision the larger row count
# wins), re-tars to a NEW name, verifies the csv-entry count, then swaps.
# The original tarball is kept as .bak and the source dir is NOT deleted —
# clean those up by hand after eyeballing the collision report.
#
# Usage (repo root, on the droplet):
#   scripts/merge_year_dir.sh <year> [office=senate]
set -euo pipefail

YEAR="${1:?usage: merge_year_dir.sh <year> [office]}"
OFFICE="${2:-senate}"
BASE="data/snapshots/${OFFICE}"
TAR="${BASE}/${YEAR}.tar.gz"
DIR="${BASE}/${YEAR}"
SCRATCH="$(mktemp -d)"
MERGED="${SCRATCH}/${YEAR}"

cleanup_scratch() { rm -rf "$SCRATCH"; }
trap cleanup_scratch EXIT

[ -f "$TAR" ] || { echo "FATAL: no tarball $TAR"; exit 1; }
[ -d "$DIR" ] || { echo "FATAL: no dir $DIR"; exit 1; }

n_tar=$(tar tzf "$TAR" | grep -c '\.csv$')
n_dir=$(find "$DIR" -maxdepth 1 -name '*.csv' | wc -l | tr -d ' ')
echo "[$OFFICE $YEAR] tarball csv=$n_tar  dir csv=$n_dir"

# 1. Extract tarball, normalise all CSVs into $MERGED (flat, one level).
mkdir -p "$MERGED"
tar xzf "$TAR" -C "$SCRATCH"
# CSVs may be at $SCRATCH/<year>/*.csv or $SCRATCH/*.csv — relocate to $MERGED.
while IFS= read -r -d '' f; do
    [ "$(dirname "$f")" = "$MERGED" ] && continue
    mv "$f" "$MERGED/"
done < <(find "$SCRATCH" -name '*.csv' -print0)
n_extracted=$(find "$MERGED" -maxdepth 1 -name '*.csv' | wc -l | tr -d ' ')
[ "$n_extracted" -eq "$n_tar" ] || { echo "FATAL: extracted $n_extracted != tar $n_tar"; exit 1; }

# 2. Fold in the dir: NEW files copied; collisions keep the larger row count.
new=0; kept_dir=0; kept_tar=0; collisions=""
while IFS= read -r -d '' f; do
    b="$(basename "$f")"
    t="${MERGED}/${b}"
    if [ ! -e "$t" ]; then
        cp "$f" "$t"; new=$((new+1))
    else
        rd=$(wc -l < "$f"); rt=$(wc -l < "$t")
        collisions+=$'\n'"    COLLISION: ${b}  dir_rows=${rd}  tar_rows=${rt}"
        if [ "$rd" -gt "$rt" ]; then cp "$f" "$t"; kept_dir=$((kept_dir+1));
        else kept_tar=$((kept_tar+1)); fi
    fi
done < <(find "$DIR" -maxdepth 1 -name '*.csv' -print0)

n_merged=$(find "$MERGED" -maxdepth 1 -name '*.csv' | wc -l | tr -d ' ')
echo "[$OFFICE $YEAR] new=$new collisions=$((kept_dir+kept_tar)) (kept_dir=$kept_dir kept_tar=$kept_tar)"
echo "[$OFFICE $YEAR] merged csv=$n_merged  (expected $((n_tar + new)))"
[ -n "$collisions" ] && echo "[$OFFICE $YEAR] collision detail:$collisions"
[ "$n_merged" -eq "$((n_tar + new))" ] || { echo "FATAL: merged count mismatch"; exit 1; }

# 3. Re-tar to a NEW name, verify, then swap (keep original as .bak).
tar czf "${TAR}.new" -C "$SCRATCH" "$YEAR"
n_newtar=$(tar tzf "${TAR}.new" | grep -c '\.csv$')
[ "$n_newtar" -eq "$n_merged" ] || { echo "FATAL: new tarball $n_newtar != merged $n_merged"; rm -f "${TAR}.new"; exit 1; }
cp "$TAR" "${TAR}.bak"
mv "${TAR}.new" "$TAR"

echo "[$OFFICE $YEAR] DONE: ${TAR} now has $n_newtar csv (was $n_tar). Original kept at ${TAR}.bak"
echo "[$OFFICE $YEAR] NEXT (after you verify): rm -rf '${DIR}' '${TAR}.bak'"
