#!/bin/bash
#
# Wrapper script for running finance-toolkit CLI
#
# Usage:
#   bin/finance-toolkit.sh move
#   bin/finance-toolkit.sh merge
#   bin/finance-toolkit.sh -X move  # with debug logging
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

source "$PROJECT_ROOT/venv/bin/activate"
python -m finance_toolkit "$@"
