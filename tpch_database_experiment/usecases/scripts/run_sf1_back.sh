#!/usr/bin/env bash
# Compatibility launcher; the use-case workflow is in run_sf1.py.
set -euo pipefail
source ~/neo4j/env.sh
exec python "$(dirname "$0")/run_sf1.py"
