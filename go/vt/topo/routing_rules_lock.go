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
)

type routingRules struct{}

var _ iTopoLock = (*routingRules)(nil)

func (s *routingRules) Type() string {
	return RoutingRulesPath
}

func (s *routingRules) ResourceName() string {
	return RoutingRulesPath
}

func (s *routingRules) Path() string {
	return RoutingRulesPath
}

// LockRoutingRules acquires a lock for routing rules.
func (ts *Server) LockRoutingRules(ctx context.Context, action string, opts ...LockOption) (context.Context, func(*error), error) {
	return ts.internalLock(ctx, &routingRules{}, action, opts...)
}

// CheckRoutingRulesLocked checks if a lock for routing rules is still possessed.
func CheckRoutingRulesLocked(ctx context.Context) error {
	return checkLocked(ctx, &routingRules{})
}
