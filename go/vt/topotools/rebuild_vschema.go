/*
Copyright 2017 Google Inc.

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

package topotools

import (
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// RebuildVSchema rebuilds the SrvVSchema for the provided cell list
// (or all cells if cell list is empty).
func RebuildVSchema(ctx context.Context, log logutil.Logger, ts *topo.Server, cells []string) error {
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
			if topo.IsErrType(err, topo.NoNode) {
				err = nil
				k = &vschemapb.Keyspace{}
			}

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				log.Errorf2(err, "GetVSchema(%v) failed", keyspace)
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
				log.Errorf2(err, "UpdateSrvVSchema(%v) failed", cell)
				mu.Lock()
				finalErr = err
				mu.Unlock()
			}
		}(cell)
	}
	wg.Wait()

	return finalErr
}
