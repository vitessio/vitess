// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"

	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// RebuildVSchema rebuilds the SrvVSchema for the provided cell list
// (or all cells if cell list is empty).
func RebuildVSchema(ctx context.Context, log logutil.Logger, ts topo.Server, cells []string) error {
	// get the actual list of cells
	if len(cells) == 0 {
		var err error
		cells, err = ts.GetKnownCells(ctx)
		if err != nil {
			return fmt.Errorf("GetKnownCells failed: %v", err)
		}
	}

	// get the keyspaces
	keyspaces, err := ts.GetKeyspaces(ctx)
	if err != nil {
		return fmt.Errorf("GetKeyspaces failed: %v", err)
	}

	// build the SrvVSchema in parallel, protected by mu
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	var finalErr error
	srvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{},
	}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()

			k, err := ts.GetVSchema(ctx, keyspace)
			if err == topo.ErrNoNode {
				err = nil
				k = &vschemapb.Keyspace{}
			}

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				log.Errorf("GetVSchema(%v) failed: %v", keyspace, err)
				finalErr = err
				return
			}
			srvVSchema.Keyspaces[keyspace] = k
		}(keyspace)
	}
	wg.Wait()
	if finalErr != nil {
		return finalErr
	}

	// now save the SrvVSchema in all cells in parallel
	for _, cell := range cells {
		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			if err := ts.UpdateSrvVSchema(ctx, cell, srvVSchema); err != nil {
				log.Errorf("UpdateSrvVSchema(%v) failed: %v", cell, err)
				mu.Lock()
				finalErr = err
				mu.Unlock()
			}
		}(cell)
	}
	wg.Wait()

	return finalErr
}
