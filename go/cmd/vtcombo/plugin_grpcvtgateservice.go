package main

// Imports and register the gRPC vtgateservice server

import (
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateservice"
)
