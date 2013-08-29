// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"github.com/youtube/vitess/go/zk"
)

/*
This file contains the cell management methods of zktopo.Server
*/

func (zkts *Server) GetKnownCells() ([]string, error) {
	cellsWithGlobal := zk.ZkKnownCells(false)
	cells := make([]string, 0, len(cellsWithGlobal))
	for _, cell := range cellsWithGlobal {
		if cell != "global" {
			cells = append(cells, cell)
		}
	}
	return cells, nil
}
