#!/bin/bash
# Third submission of the one-stage seed-vary repair, 2026-09-17.
# Submission 23648499 was cancelled while still pending: its target list came
# from the partial audit (571 chunks, 33 windows). The complete 587-chunk audit
# holds 36 cap-limited windows, so this submission uses the full target list.
set -euo pipefail
cd /nfs/hive/scratch/hhilbig/candidate_extraction
B="$PWD/build/hive_candidate_extraction"
sbatch \
  --partition=low \
  --gpus=6000_blackwell:1 \
  --cpus-per-task=2 \
  --mem=96G \
  --time=04:00:00 \
  --job-name=onestage-seedrepair3 \
  --output="/home/hhilbig/candidate-extract-seedrepair3_%j.log" \
  --export=ALL,PROJECT_ROOT="$PWD",INPUT_DIR="$B/v2_1_full_export_20260824p/chunks",OUTPUT_DIR="$B/v2_1_seed_vary_repair_20260917a",SOURCE_MANIFEST="$B/v2_1_full_export_20260824p/manifest.json",SCHEMA_VERSION=2.1,INFERENCE_SEED=20260824,MAX_OUTPUT_TOKENS=24576,REPAIR_TARGETS="$B/candidate_extraction_repair_targets_full_20260917.json",REPAIR_SEED=20260917 \
  scripts/hive/run_candidate_extraction_pilot.sbatch
