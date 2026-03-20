#!/bin/bash
set -euo pipefail

GRPC_PORT=${GRPC_PORT:-"15999"}
CELL=${CELL:-"test"}

until /vt/bin/vtctldclient --server "vtctld:$GRPC_PORT" GetCellInfoNames 2>/dev/null; do
  echo "Waiting for vtctld to be reachable..."
  sleep 1
done

echo "vtctld is reachable, creating cell $CELL..."
/vt/bin/vtctldclient --server "vtctld:$GRPC_PORT" AddCellInfo \
  --root "/vitess/$CELL" --server-address etcd:2379 "$CELL" || true
echo "Cell $CELL ready"
