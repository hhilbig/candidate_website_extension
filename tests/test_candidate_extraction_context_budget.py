from __future__ import annotations

from scripts.audit_candidate_extraction_context_budget import audit_context_budget


class _CharacterTokenizer:
    def encode(self, text: str) -> list[int]:
        return list(range(len(text)))

    def __call__(self, texts: list[str], **_: object) -> dict[str, list[int]]:
        return {"length": [len(text) for text in texts]}


def test_context_budget_counts_full_runner_message_and_fails_overflow() -> None:
    rows = [
        {
            "document_id": "issue",
            "parent_document_id": "parent-issue",
            "candidate_cycle_id": "C-1",
            "candidate_icpsr": "I-1",
            "route": "issues",
            "source_text": 'Policy "A".',
            "office": "U.S. House",
            "state": "MO",
            "year": 2006,
            "party": "D",
            "source_urls": ["https://example.test/issues"],
        },
        {
            "document_id": "endorsement",
            "parent_document_id": "parent-endorsement",
            "candidate_cycle_id": "C-2",
            "candidate_icpsr": "I-2",
            "route": "endorsements",
            "source_text": "Jane Doe",
            "office": "U.S. House",
            "state": "MO",
            "year": 2006,
            "party": "D",
            "source_urls": ["https://example.test/endorsements"],
            "endorsement_context_provenance": {
                "context_id": "context-1",
                "source_scope": "page_or_section_route_context",
            },
        },
    ]
    report = audit_context_budget(
        rows,
        _CharacterTokenizer(),
        max_model_len=1_000,
        max_output_tokens=100,
        reserved_tokens=50,
        batch_size=1,
    )
    assert report["rows"] == 2
    assert report["rows_by_route"] == {"endorsements": 1, "issues": 1}
    assert report["over_budget_rows"] >= 1
    assert report["largest_total_context_rows"][0][
        "total_reserved_context_tokens"
    ] > 1_000
