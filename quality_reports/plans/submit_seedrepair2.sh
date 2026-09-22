#!/bin/bash
# Resubmit the one-stage seed-vary repair after the 2026-09-17 timeout.
# The repair scan now accepts a cap-limited repair row as terminal, so the run
# skips the 19 published chunks and finishes the remaining 11.
set -euo pipefail
cd /nfs/hive/scratch/hhilbig/candidate_extraction
B="$PWD/build/hive_candidate_extraction"
sbatch \
  --partition=low \
  --gpus=6000_blackwell:1 \
  --cpus-per-task=2 \
  --mem=96G \
  --time=04:00:00 \
  --job-name=onestage-seedrepair2 \
  --output="/home/hhilbig/candidate-extract-seedrepair2_%j.log" \
  --export=ALL,PROJECT_ROOT="$PWD",INPUT_DIR="$B/v2_1_full_export_20260824p/chunks",OUTPUT_DIR="$B/v2_1_seed_vary_repair_20260917a",SOURCE_MANIFEST="$B/v2_1_full_export_20260824p/manifest.json",SCHEMA_VERSION=2.1,INFERENCE_SEED=20260824,MAX_OUTPUT_TOKENS=24576,REPAIR_TARGETS="$B/candidate_extraction_repair_targets_20260917.json",REPAIR_SEED=20260917 \
  scripts/hive/run_candidate_extraction_pilot.sbatch
