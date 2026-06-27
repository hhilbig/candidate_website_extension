#!/bin/bash

set -euo pipefail

JOB_NAME="candidate_website_extension house_2020"
REPO_DIR="${HOME}/Documents/GitHub/candidate_website_extension"
LOG_FILE="${REPO_DIR}/logs/house_2020_mac2.log"
PROGRESS_FILE="${REPO_DIR}/data/progress/progress_roster_house_2020.csv"
WATCHER_LOG="${REPO_DIR}/logs/house_2020_watch.log"
PID_PATTERN="src.scrape_wayback --roster data/rosters/roster_house_2020.csv"
SLACK_HELPER="${HOME}/local-party-websites-germany/scripts/slack_notify.sh"
CHECK_EVERY=300
STALL_SECONDS=3600
DIGEST_SECONDS=43200

mkdir -p "${REPO_DIR}/logs"

log_note() {
    printf '%s %s
' "$(date '+%Y-%m-%d %H:%M:%S')" "$1" >> "${WATCHER_LOG}"
}

notify() {
    local message="$1"
    if [ -f "${SLACK_HELPER}" ]; then
        # shellcheck disable=SC1090
        source "${SLACK_HELPER}"
        slack_notify "${message}"
    fi
}

get_pid() {
    pgrep -f "${PID_PATTERN}" | head -n 1 || true
}

file_mtime() {
    stat -f '%m' "$1"
}

last_activity_epoch() {
    local latest=0
    if [ -f "${LOG_FILE}" ]; then
        local ts
        ts="$(file_mtime "${LOG_FILE}")"
        if [ "${ts}" -gt "${latest}" ]; then
            latest="${ts}"
        fi
    fi
    if [ -f "${PROGRESS_FILE}" ]; then
        local ts
        ts="$(file_mtime "${PROGRESS_FILE}")"
        if [ "${ts}" -gt "${latest}" ]; then
            latest="${ts}"
        fi
    fi
    printf '%s
' "${latest}"
}

summarize_exit() {
    local status="$1"
    local tail_text="no log available"
    if [ -f "${LOG_FILE}" ]; then
        tail_text="$(tail -n 5 "${LOG_FILE}" | tr '
' ' ' | sed 's/  */ /g')"
    fi
    notify "[${HOSTNAME}] ${JOB_NAME} ${status}. log=${LOG_FILE} progress=${PROGRESS_FILE}. tail: ${tail_text}"
}

latest_progress_line() {
    if [ -f "${LOG_FILE}" ]; then
        perl -ne 'while(/([^
]*Scraping candidates:[^
]*)/g){$last=$1} END{print "$last
" if defined $last}' "${LOG_FILE}"
    fi
}

main() {
    local stalled=0
    local last_digest
    last_digest="$(date '+%s')"
    log_note "watcher_started"

    while true; do
        local pid
        pid="$(get_pid)"
        if [ -z "${pid}" ]; then
            log_note "process_not_found"
            summarize_exit "exited"
            exit 0
        fi

        local last_activity
        last_activity="$(last_activity_epoch)"
        local now
        now="$(date '+%s')"
        local idle=$((now - last_activity))

        if [ "${idle}" -ge "${STALL_SECONDS}" ] && [ "${stalled}" -eq 0 ]; then
            stalled=1
            log_note "stalled pid=${pid} idle_seconds=${idle}"
            notify "[${HOSTNAME}] ${JOB_NAME} stalled for ${idle}s. pid=${pid} log=${LOG_FILE} progress=${PROGRESS_FILE}"
        fi

        if [ "${idle}" -lt "${STALL_SECONDS}" ] && [ "${stalled}" -eq 1 ]; then
            stalled=0
            log_note "resumed pid=${pid} idle_seconds=${idle}"
            notify "[${HOSTNAME}] ${JOB_NAME} resumed. pid=${pid} idle_seconds=${idle} log=${LOG_FILE}"
        fi

        if [ $((now - last_digest)) -ge "${DIGEST_SECONDS}" ]; then
            local progress_line
            progress_line="$(latest_progress_line)"
            if [ -z "${progress_line}" ]; then
                progress_line="progress line not yet available"
            fi
            log_note "digest pid=${pid} progress=${progress_line}"
            notify "[${HOSTNAME}] ${JOB_NAME} progress digest. pid=${pid}. ${progress_line} log=${LOG_FILE} progress=${PROGRESS_FILE}"
            last_digest="${now}"
        fi

        sleep "${CHECK_EVERY}"
    done
}

main "$@"
