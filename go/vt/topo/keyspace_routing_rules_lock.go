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
)

type KeyspaceRoutingRulesLock struct {
	*TopoLock

	sourceKeyspace string
}

func NewKeyspaceRoutingRulesLock(ctx context.Context, ts *Server, sourceKeyspace string) (*KeyspaceRoutingRulesLock, error) {
	return &KeyspaceRoutingRulesLock{
		TopoLock: &TopoLock{
			Root:   "", // global
			Key:    KeyspaceRoutingRulesFile,
			Action: "KeyspaceRoutingRulesLock",
			Name:   fmt.Sprintf("KeyspaceRoutingRules for %s", sourceKeyspace),
			ts:     ts,
		},
	}, nil
}
