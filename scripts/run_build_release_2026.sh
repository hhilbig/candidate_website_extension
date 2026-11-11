#!/bin/zsh
# Build the 2026 release files from the final collection. Order: corpus, universe, panels, pairs.
# The panels need .venv-release (topic classifier). About 40 minutes.
cd "$(dirname "$0")/.." || exit 1
I=data/collection_2026/final_20261109
N=build/release_v5_0_2026
J=release-2026-build
status() { jobstatus set $J --project "candidate websites" --task "Build the 2026 release files" --total 4 --unit steps --log "${LOG:-}" "$@" }
fail() { status --state failed --note "$1"; exit 1 }
status --done 0 --note "corpus"
.venv/bin/python scripts/build_2026_release_corpus.py --input $I --crosswalk build/release_v4_0_candidate/candidate_crosswalk.csv --out $N || fail corpus
status --done 1 --note "universe"
.venv/bin/python scripts/build_2026_release_universe.py --input $I --out $N --crosswalk build/release_v4_0_candidate/candidate_crosswalk.csv || fail universe
status --done 2 --note "panels"
.venv-release/bin/python scripts/build_2026_release_panels.py --input $I --out $N || fail panels
status --done 3 --note "pairs"
.venv/bin/python scripts/build_2026_release_pairs.py --input $I --release $N || fail pairs
status --done 4 --state finished --note "complete"
echo BUILD_2026_DONE
