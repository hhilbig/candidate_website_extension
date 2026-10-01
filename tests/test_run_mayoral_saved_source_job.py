from argparse import Namespace
from pathlib import Path
from scripts.run_mayoral_saved_source_job import completion_message, build_command, alert_message


def test_completed_notification_names_project_and_distinguishes_proposals():
    message = completion_message({"processed_sources":120,"never_attempted_sources":0,
        "combined_roster":{"new_active_candidate_years_with_source_attributed_leads":5,
                           "active_candidate_years_with_url_leads":1043}},0,"mac2","/run")
    assert message.startswith("FYI")
    assert "candidate_website_extension" in message and "Mayoral URL discovery" in message
    assert "discovery proposals" in message and "5" in message
    assert "/run/source_pass.log" in message


def test_incomplete_or_crashed_job_requests_action():
    assert completion_message({"never_attempted_sources":10},0,"mac2","/run").startswith("ACTION NEEDED")
    assert completion_message({},1,"mac2","/run").startswith("ACTION NEEDED")


def test_tranche_limits_reach_child_command():
    args = Namespace(workers=8, tranche_id="approved", additional_bytes=4_000_000_000,
                     max_sources=5000, min_free_bytes=8_000_000_000)
    command = build_command(args, Path("/run"))
    for flag, value in [("--tranche-id", "approved"), ("--additional-bytes", "4000000000"),
                        ("--max-sources", "5000"), ("--min-free-bytes", "8000000000")]:
        assert command[command.index(flag)+1] == value


def test_alert_has_six_labelled_lines():
    lines = alert_message("FYI, no action needed", "Started.", "Unknown.", "droplet", "/run").splitlines()
    assert len(lines) == 6
    assert [line.split(":", 1)[0] for line in lines[1:]] == ["Project", "Task", "Status", "ETA", "Where"]
