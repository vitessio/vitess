// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"sort"

	"github.com/youtube/vitess/go/zk"
	"golang.org/x/net/context"
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
		if cell != "global" {
			cells = append(cells, cell)
		}
	}
	sort.Strings(cells)
	return cells, nil
}
