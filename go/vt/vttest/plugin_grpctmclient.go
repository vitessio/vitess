package vttest

// Imports and register the gRPC tabletmanager client

import (
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient" // nolint:revive
)
