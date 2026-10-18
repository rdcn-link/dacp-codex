#!/usr/bin/env bash
set -euo pipefail
export DACP_TEST_REMOTE_REPOSITORY=1
exec "$(dirname "$0")/test-collaboration.sh"
