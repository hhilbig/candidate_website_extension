#!/bin/bash
# Launch the approved source tranche from its restored deployment.
set -euo pipefail
deployment="$1"
runtime_python="$2"
slack_helper="$3"
export PATH="$deployment/deps/usr/bin:$PATH"
export LD_LIBRARY_PATH="$deployment/deps/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export TESSDATA_PREFIX="$deployment/deps/usr/share/tesseract-ocr/5/tessdata"
export PYTHONPATH="$deployment/deps/python:$deployment/code"
export TMPDIR="$deployment/tmp"
cd "$deployment/code"
exec "$runtime_python" scripts/run_mayoral_saved_source_job.py \
  --run-dir "$deployment/work/production" \
  --slack-helper "$slack_helper" \
  --workers 2 --tranche-id source-discovery-20260914 \
  --additional-bytes 4000000000 --max-sources 5000 --min-free-bytes 8000000000
