#!/bin/bash
# Fifth submission of the one-stage seed-vary repair, 2026-09-18.
#
# Submission history, all with the same target list and output directory:
#   23639752  low, 2 CPUs, 2h  -> TIMEOUT after 2 hours, 19 of 33 chunks written
#   23648499  low, 2 CPUs, 4h  -> CANCELLED while pending, target list superseded
#   23648684  low, 2 CPUs, 4h  -> CANCELLED while pending, target list superseded
#   23648887  high, 2 CPUs, 4h -> CANCELLED, QOSGrpCpuLimit on the shared budget
#   23649088  high, 1 CPU, 4h  -> CANCELLED, QOSGrpCpuLimit again
# `high` refuses new publicgrp work while that budget is full, so this returns to
# `low`, which is preemptible and where the first submission started in a minute.
set -euo pipefail
cd /nfs/hive/scratch/hhilbig/candidate_extraction
B="$PWD/build/hive_candidate_extraction"
sbatch \
  --partition=low \
  --gpus=6000_blackwell:1 \
  --cpus-per-task=2 \
  --mem=96G \
  --time=04:00:00 \
  --job-name=onestage-seedrepair5 \
  --output="/home/hhilbig/candidate-extract-seedrepair5_%j.log" \
  --export=ALL,PROJECT_ROOT="$PWD",INPUT_DIR="$B/v2_1_full_export_20260824p/chunks",OUTPUT_DIR="$B/v2_1_seed_vary_repair_20260917a",SOURCE_MANIFEST="$B/v2_1_full_export_20260824p/manifest.json",SCHEMA_VERSION=2.1,INFERENCE_SEED=20260824,MAX_OUTPUT_TOKENS=24576,REPAIR_TARGETS="$B/candidate_extraction_repair_targets_full_20260917.json",REPAIR_SEED=20260917 \
  scripts/hive/run_candidate_extraction_pilot.sbatch
