#!/bin/bash
# Check that the state space reductions in ../MoveTablesSwitchWrites.tla
# preserve behavior: ./check.sh <name> checks <name>.cfg.
#
# The unreduced model is taken from the commit that last had it, renamed to
# MoveTablesSwitchWritesRef. Expects tla2tools.jar in the parent directory,
# or its path in $TLA2TOOLS.
set -euo pipefail
cd "$(dirname "$0")"
REF_COMMIT=3615181
jar="$(realpath "${TLA2TOOLS:-../tla2tools.jar}")"
work=out/$1
mkdir -p "$work"
git show "$REF_COMMIT:./../MoveTablesSwitchWrites.tla" \
  | sed 's/^-* MODULE MoveTablesSwitchWrites -*$/---- MODULE MoveTablesSwitchWritesRef ----/' \
  > "$work/MoveTablesSwitchWritesRef.tla"
cp ../MoveTablesSwitchWrites.tla RefinementCheck.tla "$1.cfg" "$work/"
cd "$work"
java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -deadlock -cleanup \
  -config "$1.cfg" RefinementCheck.tla 2>&1 | tee check.log \
  | grep --line-buffered -E "violated|No error|distinct states found, 0|Finished in"
