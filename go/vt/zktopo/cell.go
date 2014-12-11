// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"sort"

	"github.com/henryanand/vitess/go/zk"
)

/*
This file contains the cell management methods of zktopo.Server
*/

func (zkts *Server) GetKnownCells() ([]string, error) {
	cellsWithGlobal, err := zk.ZkKnownCells(false)
	if err != nil {
		return cellsWithGlobal, err
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
