#!/bin/bash
# Attempt 3 of the seed-vary repair, second resume.
#
# Jobs 23717478 and 23725945 both died because the Hive home filesystem is full:
# 21 GB of 21 GB, zero bytes available. The first died mid-generation with no
# traceback after 12 of 24 chunks; the second died in one second, before Slurm
# could create its log file under /home.
#
# This submission removes the job's dependence on home. The Slurm log goes to
# scratch, and the wrapper now points VLLM_CACHE_ROOT and XDG_CACHE_HOME at
# scratch as well. Freeing space in home is a separate decision for Hanno,
# because the large directories there belong to other projects.
#
# The runner skips the 12 chunks already written and processes the remaining 12.
# Usage: bash submit_seedrepair10.sh [partition] [--test-only]
set -euo pipefail
PARTITION="${1:-low}"
shift || true
cd /nfs/hive/scratch/hhilbig/candidate_extraction
B="$PWD/build/hive_candidate_extraction"
mkdir -p "$PWD/logs"
sbatch "$@" \
  --partition="$PARTITION" \
  --gpus=6000_blackwell:1 \
  --cpus-per-task=2 \
  --mem=96G \
  --time=05:00:00 \
  --job-name=onestage-seedrepair10 \
  --output="$PWD/logs/candidate-extract-seedrepair10_%j.log" \
  --export=ALL,PROJECT_ROOT="$PWD",INPUT_DIR="$B/v2_1_full_export_20260824p/chunks",OUTPUT_DIR="$B/v2_1_seed_vary_repair_attempt3_20260918a",SOURCE_MANIFEST="$B/v2_1_full_export_20260824p/manifest.json",SCHEMA_VERSION=2.1,INFERENCE_SEED=20260824,MAX_OUTPUT_TOKENS=24576,REPAIR_TARGETS="$B/candidate_extraction_repair_targets_attempt3_20260918.json",REPAIR_SEED=20260918,REPAIR_ATTEMPT_INDEX=3 \
  scripts/hive/run_candidate_extraction_pilot.sbatch
