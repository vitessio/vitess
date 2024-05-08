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

	"vitess.io/vitess/go/vt/log"
)

// KeyspaceRoutingRulesLock is a wrapper over TopoLock, to serialize updates to the keyspace routing rules.
type KeyspaceRoutingRulesLock struct {
	*TopoLock
}

func NewKeyspaceRoutingRulesLock(ctx context.Context, ts *Server, name string) (*KeyspaceRoutingRulesLock, error) {
	if err := ts.EnsureKeyExists(ctx, "Keyspace Routing Rules", KeyspaceRoutingRulesFile); err != nil {
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
