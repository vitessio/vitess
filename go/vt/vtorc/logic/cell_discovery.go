/*
Copyright 2025 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logic

import (
	"context"
	"sync"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

var refreshCellsMu sync.Mutex

// RefreshCells refreshes the list of cells.
func RefreshCells(ctx context.Context) error {
	cellsCtx, cellsCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cellsCancel()
	cells, err := ts.GetKnownCells(cellsCtx)
	if err != nil {
		return err
	}
	return refreshCells(cells)
}

// refreshCells saves a slice of cells and removes
// stale cells that were not updated.
func refreshCells(cells []string) (err error) {
	refreshCellsMu.Lock()
	defer refreshCellsMu.Unlock()

	// save cells.
	updated := make(map[string]bool)
	for _, cell := range cells {
		err = inst.SaveCell(cell)
		if err != nil {
			log.Errorf("Failed to save cell %q: %+v", cell, err)
			return err
		}
		updated[cell] = true
	}

	// read all saved cells. the values should not
	// be changing because we are holding a lock.
	cells, err = inst.ReadCells()
	if err != nil {
		log.Errorf("Failed to read cells: %+v", err)
		return err
	}

	// delete cells that are stale.
	for _, cell := range cells {
		if updated[cell] {
			continue
		}
		log.Infof("Forgetting stale cell %q", cell)
		if err = inst.DeleteCell(cell); err != nil {
			log.Errorf("Failed to delete cell %q: %+v", cell, err)
		}
	}
	return nil
}
