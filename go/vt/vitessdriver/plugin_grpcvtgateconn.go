// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vitessdriver

// Imports and register the gRPC vtgateconn client.

import (
	// Import the gRPC vtgateconn client and make it part of the Vitess
	// Go SQL driver. This way, users do not have to do this.
	_ "github.com/youtube/vitess/go/vt/vtgate/grpcvtgateconn"
)
