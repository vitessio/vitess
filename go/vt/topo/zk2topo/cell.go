// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk2topo

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// This file contains the cell management methods of zktopo.Server.
// Eventually this will be moved to the go/vt/topo package.

// GetKnownCells is part of the topo.Server interface.
func (zs *Server) GetKnownCells(ctx context.Context) ([]string, error) {
	return zs.ListDir(ctx, topo.GlobalCell, cellsPath)
}
