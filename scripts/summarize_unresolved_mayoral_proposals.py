"""Preserve unresolved website proposals with explicit follow-up actions."""
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path

INPUT = Path('quality_reports/mayoral_ai_review_2026-09-14/validation.csv')
OUTPUT = Path('quality_reports/mayoral_unresolved_followup_2026-09-14')
ACTIONS = {
    'no_archive_capture': 'inventory_needed_availability_negative_is_not_archive_absence',
    'content_validation_unresolved': 'inspect_saved_primary_and_supplemental_content',
    'outside_headline_and_sensitivity_windows': 'seek_period_specific_evidence_preserve_out_of_period_capture',
    'outside_headline_365_within_sensitivity': 'retain_sensitivity_only_evidence_seek_headline_period',
}


def summarize(rows):
    keys = [r['candidate_url_id'] for r in rows]
    if len(keys) != len(set(keys)) or '' in keys:
        raise ValueError('Duplicate or empty proposal key')
    output = []
    for row in rows:
        if row.get('historical_campaign_content_supported', '').lower() == 'true':
            continue
        status = row['final_validation_status']
        if status not in ACTIONS:
            raise ValueError('Unknown unresolved status: ' + status)
        keep = ['candidate_url_id', 'candidate_year_id', 'full_name', 'geo_name', 'year',
                'raw_url', 'final_validation_status', 'capture_fetch_status',
                'capture_content_sha256', 'capture_effective_url', 'capture_effective_timestamp',
                'election_reference_date', 'nearest_capture_days_absolute', 'content_signal_summary',
                'supplement_raw_html_path', 'supplement_raw_html_sha256', 'supplement_effective_timestamp',
                'supplement_content_signal_summary', 'source_urls_json']
        output.append({**{key: row.get(key, '') for key in keep},
                       'next_action': ACTIONS[status], 'new_archive_request_authorized': 'False'})
    return output


def main():
    with INPUT.open() as handle:
        rows = list(csv.DictReader(handle))
    output = summarize(rows)
    assert len(rows) == 120 and len(output) == 47
    OUTPUT.mkdir(exist_ok=True)
    with (OUTPUT / 'followup.csv').open('x', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(output[0]))
        writer.writeheader()
        writer.writerows(output)
    summary = dict(input=str(INPUT), input_sha256=hashlib.sha256(INPUT.read_bytes()).hexdigest(),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        input_rows=len(rows), unresolved_rows=len(output),
        status_counts=dict(Counter(r['final_validation_status'] for r in output)),
        action_counts=dict(Counter(r['next_action'] for r in output)),
        saved_main_response_records=sum(r['capture_fetch_status'] == 'fetched' for r in output),
        supplemental_response_records=sum(bool(r['supplement_raw_html_path']) for r in output),
        limitations=['This table routes existing findings and does not establish new website ownership.',
                    'Availability-negative results do not prove archive absence.',
                    'Recorded file paths and hashes require actual byte verification before reuse.'],
        original_rows_changed=0, new_archive_requests=0)
    with (OUTPUT / 'summary.json').open('x') as handle:
        json.dump(summary, handle, indent=2)
        handle.write('\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
