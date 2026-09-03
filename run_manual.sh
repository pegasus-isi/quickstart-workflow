#!/bin/bash
#
# Manual test script for the quickstart (Hello World) workflow.
# Runs each pipeline step locally, the same way Pegasus would, before
# submitting anything to HTCondor.
#
# This validates:
# - The Python interpreter and bin/ symlinks are usable
# - Argument parsing matches what workflow_generator.py passes
# - Output files are created correctly
#
# Usage:
#   ./run_manual.sh [--use-apptainer] [--spin-time N]
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIR="${SCRIPT_DIR}/input"
OUTPUT_DIR="${SCRIPT_DIR}/test_output"
CONTAINER_IMAGE="${SCRIPT_DIR}/Quickstart_Container.sif"

USE_APPTAINER=false
SPIN_TIME=3

while [[ $# -gt 0 ]]; do
    case $1 in
        --use-apptainer)
            USE_APPTAINER=true
            shift
            ;;
        --spin-time)
            SPIN_TIME="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: $0 [--use-apptainer] [--spin-time N]"
            exit 1
            ;;
    esac
done

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }
log_step()    { echo ""; echo -e "${GREEN}========================================${NC}"; echo -e "${GREEN}STEP: $1${NC}"; echo -e "${GREEN}========================================${NC}"; }

run_cmd() {
    if [ "$USE_APPTAINER" = true ]; then
        apptainer exec --bind "${SCRIPT_DIR}:${SCRIPT_DIR}" "${CONTAINER_IMAGE}" "$@"
    else
        "$@"
    fi
}

echo ""
echo "=============================================="
echo "  Quickstart Workflow Manual Test"
echo "=============================================="
echo ""
echo "Configuration:"
echo "  Use Apptainer: ${USE_APPTAINER}"
echo "  Spin time:     ${SPIN_TIME}s"
echo "  Input Dir:     ${INPUT_DIR}"
echo "  Output Dir:    ${OUTPUT_DIR}"
echo ""

if [ "$USE_APPTAINER" = true ] && [ ! -f "${CONTAINER_IMAGE}" ]; then
    log_error "Container image not found: ${CONTAINER_IMAGE}"
    log_error "Build it with: apptainer build Quickstart_Container.sif Apptainer/Quickstart_Container.def"
    exit 1
fi

mkdir -p "${INPUT_DIR}" "${OUTPUT_DIR}"

# ==============================================================
# Step 0: Prepare test data
# ==============================================================
log_step "Preparing test data"

if [ ! -f "${INPUT_DIR}/f.in" ]; then
    log_info "Creating sample input file..."
    echo "This is the contents of the input file for the hello world workflow!" \
        > "${INPUT_DIR}/f.in"
fi
log_info "Test data ready: ${INPUT_DIR}/f.in"

# ==============================================================
# Step 1: hello
# ==============================================================
log_step "1. hello"

log_info "Running hello..."
run_cmd python3 "${SCRIPT_DIR}/bin/hello.py" \
    -T "${SPIN_TIME}" \
    -i "${INPUT_DIR}/f.in" \
    -o "${OUTPUT_DIR}/f.inter"

if [ -f "${OUTPUT_DIR}/f.inter" ]; then
    log_success "hello completed"
    ls -lh "${OUTPUT_DIR}/f.inter"
else
    log_error "hello failed — no output"
    exit 1
fi

# ==============================================================
# Step 2: world
# ==============================================================
log_step "2. world"

log_info "Running world..."
run_cmd python3 "${SCRIPT_DIR}/bin/world.py" \
    -T "${SPIN_TIME}" \
    -i "${OUTPUT_DIR}/f.inter" \
    -o "${OUTPUT_DIR}/f.out"

if [ -f "${OUTPUT_DIR}/f.out" ]; then
    log_success "world completed"
    ls -lh "${OUTPUT_DIR}/f.out"
else
    log_error "world failed — no output"
    exit 1
fi

# ==============================================================
# Summary
# ==============================================================
echo ""
echo "=============================================="
echo "  TEST COMPLETED SUCCESSFULLY!"
echo "=============================================="
echo ""
echo "Final output (${OUTPUT_DIR}/f.out):"
echo ""
cat "${OUTPUT_DIR}/f.out"
echo ""
log_success "All steps passed! Ready to run with Pegasus."
