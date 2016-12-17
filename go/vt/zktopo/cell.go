// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"sort"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
)

/*
This file contains the cell management methods of zktopo.Server
*/

// GetKnownCells is part of the topo.Server interface
func (zkts *Server) GetKnownCells(ctx context.Context) ([]string, error) {
	cellsWithGlobal, err := zk.ZkKnownCells()
	if err != nil {
		return cellsWithGlobal, convertError(err)
	}
	cells := make([]string, 0, len(cellsWithGlobal))
	for _, cell := range cellsWithGlobal {
		if cell != topo.GlobalCell {
			cells = append(cells, cell)
		}
	}
	sort.Strings(cells)
	return cells, nil
}
