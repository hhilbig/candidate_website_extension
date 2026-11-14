#!/bin/bash
#SBATCH --account=publicgrp
#SBATCH --partition=high
#SBATCH --nodes=1
#SBATCH --gpus=6000_blackwell:1
#SBATCH --cpus-per-task=2
#SBATCH --mem=96G
#SBATCH --time=01:00:00
#SBATCH --requeue
#SBATCH --job-name=candidate-extract-pilot
#SBATCH --output=/home/hhilbig/candidate-extract-pilot_%j.log

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/home/hhilbig/candidate-website-extraction}"
INPUT_DIR="${INPUT_DIR:-$PROJECT_ROOT/data/pilot/chunks}"
OUTPUT_DIR="${OUTPUT_DIR:-$PROJECT_ROOT/output/pilot}"
SOURCE_MANIFEST="${SOURCE_MANIFEST:-$(dirname "$INPUT_DIR")/manifest.json}"
EXPECTED_SOURCE_CORPUS_SHA256="${EXPECTED_SOURCE_CORPUS_SHA256:-c53d36b24b293ac5d47706aa67731f1c107d6a679cb948c9ad71a70b5c8ce7c3}"
SCHEMA_VERSION="${SCHEMA_VERSION:-2.1}"
INFERENCE_SEED="${INFERENCE_SEED:-20260824}"
MAX_OUTPUT_TOKENS="${MAX_OUTPUT_TOKENS:-24576}"
MODEL_REVISION="e89b16ebf1988b3d6befa7de50abc2d76f26eb09"
MODEL_PATH="/nfs/hive/scratch/$USER/hf/hub/models--Qwen--Qwen3.6-27B-FP8/snapshots/$MODEL_REVISION"
RUNTIME_LOCK="/nfs/hive/scratch/$USER/polsci-bakeoff-vllm-0.26.0-py312-cuda130-matched-v2.freeze.txt"
RUNTIME_LOCK_SHA256="4f46101fa493e38b312af034b8d740c612bd0001bab4cb0f624f4a9e1c49f5b3"
SCRATCH_ROOT="/scratch/$USER/candidate_extract_${SLURM_JOB_ID}"

mkdir -p "$SCRATCH_ROOT" "$OUTPUT_DIR"
export HF_HOME="/nfs/hive/scratch/$USER/hf"
export PIP_CACHE_DIR="/nfs/hive/scratch/$USER/pipcache-vllm-0.26.0"
export TMPDIR="$SCRATCH_ROOT/tmp"
export UV_INSTALL_DIR="/nfs/hive/scratch/$USER/uvbin"
export UV_PYTHON_INSTALL_DIR="/nfs/hive/scratch/$USER/uvpython-3.12"
export UV_CACHE_DIR="/nfs/hive/scratch/$USER/uvcache-vllm-0.26.0"
# Keep every cache off the home filesystem. Home is 21 GB and shared with other
# projects, and it filled on 2026-09-18. vLLM writes its torch.compile cache to
# $XDG_CACHE_HOME/vllm, which is under home by default, so a full home kills the
# job: one run died mid-generation with no traceback and the next died in one
# second before Slurm could create its log.
export VLLM_CACHE_ROOT="${VLLM_CACHE_ROOT:-/nfs/hive/scratch/$USER/vllm-cache-v0.26-cu130-matched-v2}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-/nfs/hive/scratch/$USER/xdg-cache}"
mkdir -p "$TMPDIR" "$HF_HOME" "$PIP_CACHE_DIR" "$UV_PYTHON_INSTALL_DIR" "$UV_CACHE_DIR" \
    "$VLLM_CACHE_ROOT" "$XDG_CACHE_HOME"
HOME_AVAILABLE_KB=$(df -P "$HOME" | awk 'NR==2 {print $4}')
echo "home available: ${HOME_AVAILABLE_KB} KB"

echo "=== host=$(hostname) job=$SLURM_JOB_ID start=$(date -Is) ==="
nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv,noheader

if [[ "$(sha256sum "$RUNTIME_LOCK" | awk '{print $1}')" != "$RUNTIME_LOCK_SHA256" ]]; then
    echo "runtime lock hash mismatch" >&2
    exit 3
fi
if [[ ! -d "$MODEL_PATH" || "$(basename "$MODEL_PATH")" != "$MODEL_REVISION" ]]; then
    echo "exact model snapshot is unavailable: $MODEL_PATH" >&2
    exit 3
fi
if [[ ! -f "$MODEL_PATH/config.json" ]]; then
    echo "model snapshot lacks config.json" >&2
    exit 3
fi
if [[ ! -f "$SOURCE_MANIFEST" ]]; then
    echo "source manifest is unavailable: $SOURCE_MANIFEST" >&2
    exit 3
fi
OBSERVED_SOURCE_CORPUS_SHA256=$(python3 -c \
    'import json,sys; print(json.load(open(sys.argv[1]))["source_corpus_sha256"])' \
    "$SOURCE_MANIFEST")
if [[ "$OBSERVED_SOURCE_CORPUS_SHA256" != "$EXPECTED_SOURCE_CORPUS_SHA256" ]]; then
    echo "source corpus hash mismatch" >&2
    exit 3
fi

if [[ ! -x "$UV_INSTALL_DIR/uv" ]]; then
    installed=0
    for attempt in 1 2 3 4 5; do
        if curl -LsSf --retry 5 --retry-all-errors https://astral.sh/uv/install.sh \
            | env INSTALLER_NO_MODIFY_PATH=1 sh; then
            installed=1
            break
        fi
        sleep 15
    done
    if [[ "$installed" -ne 1 ]]; then
        echo "failed to install uv after five attempts" >&2
        exit 3
    fi
fi
export PATH="$UV_INSTALL_DIR:$PATH"

uv venv "$SCRATCH_ROOT/venv" --python 3.12
source "$SCRATCH_ROOT/venv/bin/activate"
UV_FLAGS=(-q)
if [[ "${OFFLINE_INSTALL:-1}" == "1" ]]; then
    UV_FLAGS+=(--offline)
fi
uv pip install "${UV_FLAGS[@]}" --torch-backend=auto -r "$RUNTIME_LOCK"
uv pip install "${UV_FLAGS[@]}" \
    "cuda-toolkit[cccl,crt,cudart,nvcc,nvrtc,nvvm]==13.0.2" \
    "nvidia-cuda-cccl==13.0.85" \
    "nvidia-cuda-crt==13.0.88" \
    "nvidia-cuda-nvcc==13.0.88" \
    "nvidia-cuda-nvrtc==13.0.88" \
    "nvidia-cuda-runtime==13.0.96" \
    "nvidia-nvvm==13.0.88"

NVCC_PATH=$(find "$SCRATCH_ROOT/venv" -name nvcc -type f | head -1)
if [[ -z "$NVCC_PATH" ]]; then
    echo "could not find nvcc" >&2
    exit 4
fi
export PATH="$(dirname "$NVCC_PATH"):$PATH"
export CUDA_HOME="$(dirname "$(dirname "$NVCC_PATH")")"
CUDA_RUNTIME_LIB="$CUDA_HOME/lib"
if [[ ! -f "$CUDA_RUNTIME_LIB/libcudart.so.13" || ! -f "$CUDA_RUNTIME_LIB/libnvrtc.so.13" ]]; then
    echo "pinned CUDA runtime libraries are incomplete" >&2
    exit 4
fi
if [[ ! -e "$CUDA_HOME/lib64" ]]; then
    ln -s lib "$CUDA_HOME/lib64"
fi
if [[ ! -e "$CUDA_HOME/lib64/libcudart.so" ]]; then
    ln -s "$CUDA_RUNTIME_LIB/libcudart.so.13" "$CUDA_HOME/lib64/libcudart.so"
fi
if [[ ! -e "$CUDA_HOME/lib64/libnvrtc.so" ]]; then
    ln -s "$CUDA_RUNTIME_LIB/libnvrtc.so.13" "$CUDA_HOME/lib64/libnvrtc.so"
fi
export LIBRARY_PATH="$CUDA_HOME/lib64${LIBRARY_PATH:+:$LIBRARY_PATH}"
export LD_LIBRARY_PATH="$CUDA_HOME/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

CUDA_LINK_TEST="$SCRATCH_ROOT/libcuda_link_preflight.so"
c++ -shared -x c++ /dev/null -Wl,--no-as-needed \
    -L"$CUDA_HOME/lib64" -L"$CUDA_HOME/lib64/stubs" \
    -lcudart -lcuda -lnvrtc -o "$CUDA_LINK_TEST"
readelf -d "$CUDA_LINK_TEST" | grep -q 'libcudart.so.13'
readelf -d "$CUDA_LINK_TEST" | grep -q 'libcuda.so.1'
readelf -d "$CUDA_LINK_TEST" | grep -q 'libnvrtc.so.13'

EFFECTIVE_LOCK="$SCRATCH_ROOT/effective-runtime.freeze.txt"
uv pip freeze | sort > "$EFFECTIVE_LOCK"
if ! cmp -s "$EFFECTIVE_LOCK" "$RUNTIME_LOCK"; then
    echo "installed runtime differs from frozen target lock" >&2
    echo "effective_sha256=$(sha256sum "$EFFECTIVE_LOCK" | awk '{print $1}')" >&2
    echo "target_sha256=$RUNTIME_LOCK_SHA256" >&2
    exit 5
fi
uv pip check

export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1
export VLLM_USE_DEEP_GEMM=0
export VLLM_USE_FLASHINFER_SAMPLER=0
export MAX_JOBS="${SLURM_CPUS_PER_TASK:-1}"
export FLASHINFER_NVCC_THREADS=1

cd "$PROJECT_ROOT"
SHARD_ARGS=()
if [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]]; then
    if [[ -z "${SHARD_COUNT:-}" ]]; then
        echo "SHARD_COUNT is required for array jobs" >&2
        exit 6
    fi
    SHARD_ARGS+=(--shard-index "$SLURM_ARRAY_TASK_ID" --shard-count "$SHARD_COUNT")
fi
REPAIR_ARGS=()
if [[ "${COMPACT_REPAIR:-0}" == "1" ]]; then
    if [[ "$SCHEMA_VERSION" != "1.0" ]]; then
        echo "compact repair is restricted to the frozen v1 audit" >&2
        exit 6
    fi
    REPAIR_ARGS+=(--compact-repair)
fi
SEED_VARY_ARGS=()
if [[ -n "${REPAIR_TARGETS:-}" ]]; then
    if [[ -z "${REPAIR_SEED:-}" ]]; then
        echo "REPAIR_SEED is required with REPAIR_TARGETS" >&2
        exit 6
    fi
    SEED_VARY_ARGS+=(--repair-targets "$REPAIR_TARGETS" --repair-seed "$REPAIR_SEED")
    if [[ -n "${REPAIR_ATTEMPT_INDEX:-}" ]]; then
        SEED_VARY_ARGS+=(--repair-attempt-index "$REPAIR_ATTEMPT_INDEX")
    fi
fi
python scripts/hive/extract_candidate_pages_vllm.py \
    --chunks-dir "$INPUT_DIR" \
    --output-dir "$OUTPUT_DIR" \
    --model-path "$MODEL_PATH" \
    --schema-version "$SCHEMA_VERSION" \
    --seed "$INFERENCE_SEED" \
    --max-output-tokens "$MAX_OUTPUT_TOKENS" \
    --gpu-memory-utilization 0.92 \
    "${REPAIR_ARGS[@]}" \
    "${SEED_VARY_ARGS[@]}" \
    "${SHARD_ARGS[@]}"

echo "=== done job=$SLURM_JOB_ID end=$(date -Is) ==="
