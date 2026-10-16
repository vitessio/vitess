#!/bin/bash
# Model-check one configuration: ./run.sh <name> checks <name>.cfg.
# Expects tla2tools.jar next to this script, or its path in $TLA2TOOLS.
set -euo pipefail
cd "$(dirname "$0")"
jar="${TLA2TOOLS:-tla2tools.jar}"
mkdir -p out
java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -deadlock -cleanup -noGenerateSpecTE \
  -metadir "out/states_$1" -dumpTrace json "out/$1.trace.json" \
  -config "$1.cfg" MoveTablesSwitchWrites.tla 2>&1 | tee "out/$1.log" \
  | grep --line-buffered -E "violated|No error|distinct states found, 0|Finished in"
