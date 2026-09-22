#!/bin/bash
# Attempt 3 of the seed-vary repair, resumed after job 23717478 died silently.
#
# Attempt 2 (job 23655140) completed and repaired 12 of its 36 windows. This
# submission covers only the 24 windows attempt 2 left at the output cap, in a
# separate output directory, so attempt 2's rows stay byte-identical.
#
# The wrapper now forwards REPAIR_ATTEMPT_INDEX to the runner. Before this fix
# the runner kept its default of attempt 2, so this run would have recorded the
# wrong attempt index against seed 20260918.
#
# Wall time: 24 cap-limited windows at about nine minutes each is about 3.6
# hours, so the request is five hours.
#
# Resume: the runner skips the 12 chunks already written and processes the remaining 12.
# Usage: bash submit_seedrepair9.sh [partition] [--test-only]
set -euo pipefail
PARTITION="${1:-high}"
shift || true
cd /nfs/hive/scratch/hhilbig/candidate_extraction
B="$PWD/build/hive_candidate_extraction"
sbatch "$@" \
  --partition="$PARTITION" \
  --gpus=6000_blackwell:1 \
  --cpus-per-task=2 \
  --mem=96G \
  --time=05:00:00 \
  --job-name=onestage-seedrepair9 \
  --output="/home/hhilbig/candidate-extract-seedrepair9_%j.log" \
  --export=ALL,PROJECT_ROOT="$PWD",INPUT_DIR="$B/v2_1_full_export_20260824p/chunks",OUTPUT_DIR="$B/v2_1_seed_vary_repair_attempt3_20260918a",SOURCE_MANIFEST="$B/v2_1_full_export_20260824p/manifest.json",SCHEMA_VERSION=2.1,INFERENCE_SEED=20260824,MAX_OUTPUT_TOKENS=24576,REPAIR_TARGETS="$B/candidate_extraction_repair_targets_attempt3_20260918.json",REPAIR_SEED=20260918,REPAIR_ATTEMPT_INDEX=3 \
  scripts/hive/run_candidate_extraction_pilot.sbatch
