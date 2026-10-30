#!/usr/bin/env bash
# Maintained local/cross-node service tests; fixtures supply their own operators.
set -euo pipefail
cd "$(dirname "$0")/.."
(cd dacp-server && sbt -batch 'testOnly dacp.CollaborationSpec')
(cd dacp-union-server && sbt -batch 'testOnly dacp.unionserver.UnionSpec')
