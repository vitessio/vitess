package vitessdriver

// Imports and register the gRPC vtgateconn client.

import (
	// Import the gRPC vtgateconn client and make it part of the Vitess
	// Go SQL driver. This way, users do not have to do this.
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)
