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

/*
createTopoDirIfNeeded creates the keyspace routing rules key by creating a sentinel (dummy) child key under it.
Vitess expects a key to exist, and to have a child key (it imposes a directory-like structure), before locking it.
Without this we get an error when trying to lock :node doesn't exist: /vitess/global/KeyspaceRoutingRules/.
*/
func createTopoDirIfNeeded(ctx context.Context, ts *Server) error {
	sentinelPath := path.Join(KeyspaceRoutingRulesFile, "sentinel")
	_, _, err := ts.GetGlobalCell().Get(ctx, sentinelPath)
	if IsErrType(err, NoNode) {
		_, err = ts.globalCell.Create(ctx, sentinelPath, []byte("force creation of the keyspace routing rules root key"))
		if IsErrType(err, NodeExists) {
			// Another process created the file, which is fine.
			return nil
		}
		if err != nil {
			log.Errorf("Failed to create keyspace routing rules sentinel file: %v", err)
		} else {
			log.Infof("Successfully created keyspace routing rules sentinel file %s", sentinelPath)
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
			Path: KeyspaceRoutingRulesFile,
			Name: fmt.Sprintf("KeyspaceRoutingRules::%s", name),
			ts:   ts,
		},
	}, nil
}
