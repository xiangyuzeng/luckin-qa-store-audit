#!/usr/bin/env bash
#
# QA Store Audit v3 — Pipeline Runner
#
# Usage:
#   bash run.sh <inspection.xlsx> [checklist.xlsx] [YYYY-MM]
#
# Arguments:
#   $1  Path to the QA inspection Excel file (.xlsx)
#   $2  Path to QA checklist benchmark (.xlsx, optional — pass '-' to skip)
#   $3  Analysis month in YYYY-MM format (optional; auto-detects latest month)
#
# Output:
#   output/qa-analysis-YYYY-MM/
#     00_summary.json
#     01_store_performance.csv
#     02_module_analysis.csv
#     03_store_module_matrix.csv
#     04_risk_detail.csv
#     05_s_items_detail.csv
#     06_trend_data.csv
#     07_module_mapping_audit.csv
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INPUT_FILE="${1:-}"
CHECKLIST="${2:-}"
MONTH="${3:-}"
OUTPUT_DIR="${SCRIPT_DIR}/output"

if [ -z "$INPUT_FILE" ]; then
    echo "Usage: bash run.sh <inspection.xlsx> [checklist.xlsx] [YYYY-MM]"
    echo ""
    echo "  inspection.xlsx  Path to the QA inspection data Excel file"
    echo "  checklist.xlsx   Path to QA checklist benchmark (optional, pass '-' to skip)"
    echo "  YYYY-MM          Analysis month (optional, auto-detects latest month)"
    exit 1
fi

if [ ! -f "$INPUT_FILE" ]; then
    echo "ERROR: Input file not found: $INPUT_FILE"
    exit 1
fi

echo "============================================"
echo "QA Store Audit v3 Analysis"
echo "============================================"
echo "Input:     $INPUT_FILE"
echo "Checklist: ${CHECKLIST:-(none)}"
echo "Month:     ${MONTH:-auto-detect}"
echo ""

# Install dependencies if needed
pip3 install --break-system-packages -q pandas openpyxl 2>/dev/null || \
pip3 install -q pandas openpyxl 2>/dev/null || true

# Build CLI args
PYTHON_ARGS="--input $INPUT_FILE --output-dir ${OUTPUT_DIR}"
if [ -n "$CHECKLIST" ] && [ "$CHECKLIST" != "-" ] && [ -f "$CHECKLIST" ]; then
    PYTHON_ARGS="$PYTHON_ARGS --checklist $CHECKLIST"
fi
[ -n "$MONTH" ] && PYTHON_ARGS="$PYTHON_ARGS --month $MONTH"

# Run analysis
cd "${SCRIPT_DIR}"
python3 analyze/run_analysis.py $PYTHON_ARGS

echo ""
echo "============================================"
echo "Done."
echo "============================================"
