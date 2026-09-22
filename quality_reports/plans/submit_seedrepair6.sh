#!/bin/bash
# Sixth submission of the one-stage seed-vary repair, 2026-09-18.
#
# Submission history, all with the same target list and output directory:
#   23639752  low, 2 CPUs      -> TIMEOUT after 2 hours, 19 of 33 chunks written
#   23648499  low, 2 CPUs      -> CANCELLED while pending, partial target list
#   23648684  low, 2 CPUs      -> CANCELLED while pending, queue
#   23648887  high, 2 CPUs     -> CANCELLED, QOSGrpCpuLimit
#   23649088  high, 1 CPU      -> CANCELLED, QOSGrpCpuLimit
#   23649113  low, 2 CPUs      -> FAILED in 14 s, ordered row comparison in the
#                                repair scan refused two correct multi-target chunks
# This submission carries the order-free comparison. `preflight_repair_scan.py`
# reproduces the scan against the published outputs and reports 18 chunks skipped
# and 15 to process, which is 16 of the 36 targets.
set -euo pipefail
cd /nfs/hive/scratch/hhilbig/candidate_extraction
B="$PWD/build/hive_candidate_extraction"
sbatch \
  --partition=low \
  --gpus=6000_blackwell:1 \
  --cpus-per-task=2 \
  --mem=96G \
  --time=04:00:00 \
  --job-name=onestage-seedrepair6 \
  --output="/home/hhilbig/candidate-extract-seedrepair6_%j.log" \
  --export=ALL,PROJECT_ROOT="$PWD",INPUT_DIR="$B/v2_1_full_export_20260824p/chunks",OUTPUT_DIR="$B/v2_1_seed_vary_repair_20260917a",SOURCE_MANIFEST="$B/v2_1_full_export_20260824p/manifest.json",SCHEMA_VERSION=2.1,INFERENCE_SEED=20260824,MAX_OUTPUT_TOKENS=24576,REPAIR_TARGETS="$B/candidate_extraction_repair_targets_full_20260917.json",REPAIR_SEED=20260917 \
  scripts/hive/run_candidate_extraction_pilot.sbatch
