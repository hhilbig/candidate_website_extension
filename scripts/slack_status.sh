#!/bin/bash
# Portable Slack status helper for long-running jobs. See SKILL.md (job-alerts).
# Usage:
#   . scripts/slack_status.sh
#   slack_status LEVEL PROJECT TASK STATUS ETA [WHERE]
#   LEVEL is FYI or "ACTION NEEDED". SLACK_DRY_RUN=1 prints instead of posting.
# Webhook: SLACK_WEBHOOK_URL or ~/.slack_wayback_webhook (never print it).
# Repo line: git remote of the checkout, else SLACK_REPO=owner/name.
# Host line: HOST_LABEL, else a known-hostname map, else the short hostname.

slack_webhook_url() {
    if [ -n "${SLACK_WEBHOOK_URL:-}" ]; then printf "%s" "$SLACK_WEBHOOK_URL"; return 0; fi
    if [ -f "$HOME/.slack_wayback_webhook" ]; then tr -d '\r\n' < "$HOME/.slack_wayback_webhook"; return 0; fi
    return 1
}

json_escape() { python3 -c 'import json, sys; print(json.dumps(sys.stdin.read())[1:-1])'; }

slack_notify() {
    local message="$1"
    if [ -n "${SLACK_DRY_RUN:-}" ]; then printf '%s\n' "$message"; return 0; fi
    local webhook
    webhook="$(slack_webhook_url)" || return 0
    local escaped
    escaped="$(printf "%s" "$message" | json_escape)"
    curl -fsS -X POST -H "Content-Type: application/json" \
        --data "{\"text\":\"$escaped\"}" "$webhook" >/dev/null 2>&1 || true
}

slack_repo_name() {
    local url
    url="$(git -C "$(dirname "${BASH_SOURCE[0]}")" config --get remote.origin.url 2>/dev/null || true)"
    if [ -n "$url" ]; then
        url="${url%.git}"; url="${url#git@github.com:}"; url="${url#https://github.com/}"
        printf "github.com/%s" "$url"
    elif [ -n "${SLACK_REPO:-}" ]; then
        printf "github.com/%s" "$SLACK_REPO"
    else
        printf "repo unknown"
    fi
}

slack_host_label() {
    if [ -n "${HOST_LABEL:-}" ]; then printf "%s" "$HOST_LABEL"; return; fi
    case "$(hostname -s 2>/dev/null || hostname)" in
        LT63LF9P2M) printf "mac2" ;;
        *) printf "%s" "$(hostname -s 2>/dev/null || hostname)" ;;
    esac
}

slack_status() {
    local level="$1" project="$2" task="$3" state="$4" eta="$5" where="${6:-}"
    local head
    case "$level" in
        FYI) head="FYI, no action needed" ;;
        *) head="ACTION NEEDED" ;;
    esac
    local loc="$(slack_host_label), repo $(slack_repo_name)"
    [ -n "$where" ] && loc="$loc, $where"
    slack_notify "$head
Project: $project
Task: $task
Status: $state
ETA: $eta
Where: $loc"
}
