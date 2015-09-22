// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vttest

import (
	"errors"
	"os"
	"path"

	// we use gRPC everywhere, so import the vtgate client.
	_ "github.com/youtube/vitess/go/vt/vtgate/grpcvtgateconn"
)

func launcherPath() (string, error) {
	vttop := os.Getenv("VTTOP")
	if vttop == "" {
		return "", errors.New("VTTOP not set")
	}
	return path.Join(vttop, "py/vttest/run_local_database.py"), nil
}

func vtgateProtocol() string {
	return "grpc"
}
