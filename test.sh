#!/usr/bin/env bash
#
# Manual test script for the download queue manager feature.
# Runs build, vet and all unit tests related to the queue manager,
# the downloader pause/resume integration and the existing packages.
#
# Usage:
#   ./test.sh            run all checks
#   ./test.sh queue      run only the queue manager unit tests
#   ./test.sh pause      run only the pause/resume unit tests
#
# Note: extractor tests (./extractors/...) and downloader network tests
# require internet access and are not included here.

set -euo pipefail
cd "$(dirname "$0")"

run_all() {
    echo "==> go build ./..."
    go build ./...

    echo "==> go vet ./app/... ./downloader/..."
    go vet ./app/... ./downloader/...

    echo "==> gofmt check"
    unformatted=$(gofmt -l app downloader)
    if [ -n "$unformatted" ]; then
        echo "gofmt: the following files are not formatted:"
        echo "$unformatted"
        exit 1
    fi

    run_queue
    run_pause

    echo "==> go test (existing local unit tests)"
    # TestGetNameAndExt requires internet access (it fetches the URL to
    # detect the content type), skip it for offline runs.
    go test -count=1 -skip 'TestGetNameAndExt' ./utils/... ./parser/... ./config/...

    echo
    echo "All checks passed."
}

run_queue() {
    echo "==> queue manager unit tests (app package)"
    go test -count=1 -v -run "TestQueue|TestNewQueueManager|TestTaskLess" ./app/...

    echo "==> queue manager unit tests with race detector"
    go test -count=1 -race -run "TestQueue|TestNewQueueManager|TestTaskLess" ./app/...
}

run_pause() {
    echo "==> pause/resume unit tests (downloader package)"
    go test -count=1 -v -run "TestPause|TestDownloaderWaitIfPaused" ./downloader/...

    echo "==> pause/resume unit tests with race detector"
    go test -count=1 -race -run "TestPause|TestDownloaderWaitIfPaused" ./downloader/...
}

case "${1:-all}" in
    all)   run_all ;;
    queue) run_queue ;;
    pause) run_pause ;;
    *)
        echo "unknown target: $1 (expected: all | queue | pause)" >&2
        exit 1
        ;;
esac
