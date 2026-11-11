#!/bin/zsh
# Packager and validator for the 2026 extraction bundle (first 2026 version).
cd /Users/hanno/Documents/GitHub/candidate_website_extension
export CANDIDATE_EXTRACTION_SCHEMA_VERSION=2.2
export CANDIDATE_EXTRACTION_BUNDLE=config/candidate_extraction_bundle_2026.json
OUT=build/hive_candidate_extraction/release_2026_20261111_v22_model
MC=quality_reports/candidate_extraction_model_coded_validation_2026-11-11.json
B=build/hive_candidate_extraction/bundle_2026_20261111_v22
C=build/release_v5_0_2026_core
rm -rf $OUT
echo "build start $(date)"
.venv/bin/python scripts/build_candidate_extraction_release.py --core-release-dir $C --bundle-dir $B --output-dir $OUT --model-coded-validation $MC > $OUT.summary.json 2> $OUT.stderr
rc=$?; echo "build_exit=$rc $(date)"; [ $rc -ne 0 ] && { tail -3 $OUT.stderr; exit 1; }
.venv/bin/python scripts/validate_candidate_extraction_release.py $OUT --core-release-dir $C --bundle-dir $B --manifest $OUT/candidate_extraction_manifest.json --model-coded-validation $MC > $OUT.validate.json 2> $OUT.validate.stderr
rc=$?; echo "validate_exit=$rc $(date)"; [ $rc -ne 0 ] && { tail -3 $OUT.validate.stderr; exit 1; }
ls -la $OUT
