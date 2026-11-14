#!/bin/zsh
# Assemble release version 5.0 from the published version 4.0 files and the 2026 build.
# Order: base files, pair file, extracted-variable files, on_ballot correction, cross-file check.
cd "$(dirname "$0")/.." || exit 1
P=build/release_v4_0_candidate
N=build/release_v5_0_2026
X=build/hive_candidate_extraction/release_2026_20261111_v22_model
O=build/release_v5_0_candidate
PY=.venv/bin/python
$PY scripts/assemble_release_v5_0.py --published $P --new $N --out $O || exit 1
mkdir -p build/release_v5_0_reports && mv $O/assemble_report.json build/release_v5_0_reports/
cp $N/event_pairs_2026.csv $O/ || exit 1
$PY scripts/assemble_release_v5_0_extraction.py --published $P --new $X --corpus-2026 $N/raw_corpus_2026.parquet \
    --out $O --also event_pairs_2026.csv --also on_ballot_2026_sources.csv || exit 2
$PY scripts/correct_on_ballot_v5_0.py $O --report quality_reports/on_ballot_correction_v5_0.csv || exit 3
$PY scripts/validate_release_v5_0.py $O || exit 4
echo ASSEMBLE_DONE
