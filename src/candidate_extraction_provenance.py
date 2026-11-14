"""Stable source-transform and offset provenance for candidate extraction."""

SOURCE_TEXT_TRANSFORM_VERSION = "candidate_extraction_source_text_v1"
SOURCE_TEXT_TRANSFORM_RULE = (
    "Before segmentation and inference, replace every non-overlapping literal "
    "three-code-point sequence #+# with one U+000A LINE FEED using Python str.replace."
)
OFFSET_COORDINATE_SYSTEM = (
    "Zero-based, half-open Python Unicode code-point indices in the transformed "
    "extraction source text, not in raw text_snap_content."
)
