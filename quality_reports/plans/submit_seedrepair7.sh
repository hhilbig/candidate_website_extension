#!/bin/bash
# Seventh submission of the first seed-vary repair (attempt 2, seed 20260917).
#
# 23654548 pended on `low` with a predicted start of 2026-09-18T14:48, because the
# account's fairshare is 0.01-0.04 while two reind-dir-screen jobs hold it. The
# same job on `high` predicts 2026-09-18T05:01, so this submission moves it there.
# Everything else is unchanged, including the target list and the output directory.
set -euo pipefail
cd /nfs/hive/scratch/hhilbig/candidate_extraction
B="$PWD/build/hive_candidate_extraction"
sbatch \
  --partition=high \
  --gpus=6000_blackwell:1 \
  --cpus-per-task=2 \
  --mem=96G \
  --time=04:00:00 \
  --job-name=onestage-seedrepair7 \
  --output="/home/hhilbig/candidate-extract-seedrepair7_%j.log" \
  --export=ALL,PROJECT_ROOT="$PWD",INPUT_DIR="$B/v2_1_full_export_20260824p/chunks",OUTPUT_DIR="$B/v2_1_seed_vary_repair_20260917a",SOURCE_MANIFEST="$B/v2_1_full_export_20260824p/manifest.json",SCHEMA_VERSION=2.1,INFERENCE_SEED=20260824,MAX_OUTPUT_TOKENS=24576,REPAIR_TARGETS="$B/candidate_extraction_repair_targets_full_20260917.json",REPAIR_SEED=20260917,REPAIR_ATTEMPT_INDEX=2 \
  scripts/hive/run_candidate_extraction_pilot.sbatch
