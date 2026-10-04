#!/bin/bash
# run1.sh <cfg> [extra TLC args]: runs TLC on one configuration and prints the result summary.
cd "$(dirname "$0")"
cfg=$1; shift
mkdir -p out
java -XX:+UseParallelGC -Xmx${TLC_HEAP:-8g} -cp ${TLA2TOOLS_JAR:-tla2tools.jar} tlc2.TLC -noGenerateSpecTE -workers ${TLC_WORKERS:-4} -config $cfg.cfg \
  -metadir out/states-$cfg "$@" SemiSyncFailover.tla > out/$cfg.out 2>&1
grep -E "^Error: Invariant|is violated|No error has been found|states generated|depth of the complete|Finished in|^Error" out/$cfg.out | grep -v "^Picked"
