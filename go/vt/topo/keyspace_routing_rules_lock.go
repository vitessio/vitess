/*
Copyright 2024 The Vitess Authors.

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

package topo

import (
	"context"
	"fmt"
	"path"

	"vitess.io/vitess/go/vt/log"
)

// KeyspaceRoutingRulesLock is a wrapper over TopoLock, to serialize updates to the keyspace routing rules.
type KeyspaceRoutingRulesLock struct {
	*TopoLock
}

func createTopoDirIfNeeded(ctx context.Context, ts *Server) error {
	// We need the directory to exist since etcd (for example) will create a file there as part of its locking
	// mechanism. So we create it if it doesn't exist. The file created below "lock" is just a dummy file.
	// If no dummy file is specified, the directory doesn't get created (etcd behavior).
	topoPath := path.Join(KeyspaceRoutingRulesPath, "lock")
	_, _, err := ts.GetGlobalCell().Get(ctx, topoPath)
	if IsErrType(err, NoNode) {
		log.Infof("Creating keyspace routing rules file %s", topoPath)
		_, err = ts.globalCell.Create(ctx, topoPath, []byte("lock file for keyspace routing rules"))
		if err != nil {
			log.Errorf("Failed to create keyspace routing rules lock file: %v", err)
		}
	}
	return err
}

func NewKeyspaceRoutingRulesLock(ctx context.Context, ts *Server, name string) (*KeyspaceRoutingRulesLock, error) {
	if err := createTopoDirIfNeeded(ctx, ts); err != nil {
		log.Errorf("Failed to create keyspace routing rules lock file: %v", err)
		return nil, err
	}

	return &KeyspaceRoutingRulesLock{
		TopoLock: &TopoLock{
			Path: KeyspaceRoutingRulesPath,
			Name: fmt.Sprintf("KeyspaceRoutingRules:: %s", name),
			ts:   ts,
		},
	}, nil
}
