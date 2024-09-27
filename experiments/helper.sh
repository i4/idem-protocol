#!/bin/bash

set +e

HOME="$(cd "$(dirname "$0")" && pwd)"
EXPERIMENT_NAME="$(basename "$HOME")"
MAIN_DIR="$(cd ~/../.. && pwd)"
err_code=1
check=""

config_files=( "scripts/config/java" "scripts/config/refit-overrides" )

if [[ "$1" == "--check" ]]; then
  check="--check"
fi

function cleanup() {
    # only try to restore a bckp file if it actually exists
    for i in "${config_files[@]}"; do
        if [[ -f "$MAIN_DIR/$i.bckp" ]] ; then
            mv "$MAIN_DIR/$i.bckp" "$MAIN_DIR/$i"
        fi
    done
    exit $err_code
}

echo "Running experiment ${EXPERIMENT_NAME} for ${EXPERIMENT_DURATION} seconds each"

# setup experiment configuration
# delete old files to avoid resurrecting old versions
for i in "${config_files[@]}"; do
    rm -f "$MAIN_DIR/$i.bckp"
done
# handle interrupts and errors
trap cleanup INT
trap cleanup ERR
# backup original files, then copy the new ones
for i in "${config_files[@]}"; do
    fn="$(basename "$i")"
    if [[ -f ~/"$fn" ]] ; then
        mv "$MAIN_DIR/$i" "$MAIN_DIR/$i.bckp"
        cp ~/"$fn" "$MAIN_DIR/$i"
    fi
done

run_analysis=()
for i in "${ANALYSIS[@]}"; do
  run_analysis+=( "-a" "$i" )
done

# queue experiment
cd "$MAIN_DIR/scripts" || exit 1
exp/exp remote $check "${run_analysis[@]}" -m "$SCRIPT" "$EXPERIMENT_NAME" "${EXPERIMENT_EXTRA[@]}" "$EXPERIMENT_DURATION"

# cleanup
err_code=0
cleanup
