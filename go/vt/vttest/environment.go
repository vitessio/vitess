// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vttest

import (
	"errors"
	"os"
	"path"
)

func launcherPath() (string, error) {
	vttop := os.Getenv("VTTOP")
	if vttop == "" {
		return "", errors.New("VTTOP not set")
	}
	return path.Join(vttop, "py/vttest/run_local_database.py"), nil
}

func topoFlags(topo string) []string {
	return []string{"--topology", topo}
}

func vtgateProtocol() string {
	return "grpc"
}
