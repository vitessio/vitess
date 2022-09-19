/*
Copyright 2022 The Vitess Authors.

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
	"sort"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

// RefreshAllKeyspaces reloads the keyspace information for the keyspaces that vtorc is concerned with.
func RefreshAllKeyspaces() {
	var keyspaces []string
	if *clustersToWatch == "" { // all known keyspaces
		ctx, cancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
		defer cancel()
		var err error
		// Get all the keyspaces
		keyspaces, err = ts.GetKeyspaces(ctx)
		if err != nil {
			log.Error(err)
			return
		}
	} else {
		// Parse input and build list of keyspaces
		inputs := strings.Split(*clustersToWatch, ",")
		for _, ks := range inputs {
			if strings.Contains(ks, "/") {
				// This is a keyspace/shard specification
				input := strings.Split(ks, "/")
				keyspaces = append(keyspaces, input[0])
			} else {
				// Assume this is a keyspace
				keyspaces = append(keyspaces, ks)
			}
		}
		if len(keyspaces) == 0 {
			log.Errorf("Found no keyspaces for input: %v", *clustersToWatch)
			return
		}
	}

	// Sort the list of keyspaces.
	// The list can have duplicates because the input to clusters to watch may have multiple shards of the same keyspace
	sort.Strings(keyspaces)
	refreshCtx, refreshCancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
	defer refreshCancel()
	var wg sync.WaitGroup
	for idx, keyspace := range keyspaces {
		// Check if the current keyspace name is the same as the last one.
		// If it is, then we know we have already refreshed its information.
		// We do not need to do it again.
		if idx != 0 && keyspace == keyspaces[idx-1] {
			continue
		}
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			_ = refreshKeyspace(refreshCtx, keyspace)
		}(keyspace)
	}
	wg.Wait()
}

// RefreshKeyspace refreshes the keyspace's information for the given keyspace from the topo
func RefreshKeyspace(keyspaceName string) error {
	refreshCtx, refreshCancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
	defer refreshCancel()
	return refreshKeyspace(refreshCtx, keyspaceName)
}

// refreshKeyspace is a helper function which reloads the given keyspace's information
func refreshKeyspace(ctx context.Context, keyspaceName string) error {
	keyspaceInfo, err := ts.GetKeyspace(ctx, keyspaceName)
	if err != nil {
		log.Error(err)
		return err
	}
	err = inst.SaveKeyspace(keyspaceInfo)
	if err != nil {
		log.Error(err)
	}
	return err
}
