/*
Copyright 2021 The Vitess Authors.

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

package rbac

import (
	"fmt"

	"vitess.io/vitess/go/sets"
)

// Rule is a single rule governing access to a particular resource.
type Rule struct {
	clusters sets.Set[string]
	actions  sets.Set[string]
	subjects sets.Set[string]
}

// Allows returns true if the actor is allowed to take the specified action in
// the specified cluster.
//
// A nil actor signifies the unauthenticated state, and is only allowed access
// if the rule contains the wildcard ("*") subject.
func (r *Rule) Allows(clusterID string, action Action, actor *Actor) bool {
	if r.clusters.HasAny("*", clusterID) {
		if r.actions.HasAny("*", string(action)) {
			if r.subjects.Has("*") {
				return true
			}

			if actor == nil {
				return false
			}

			if r.subjects.Has(fmt.Sprintf("user:%s", actor.Name)) {
				return true
			}

			for _, role := range actor.Roles {
				if r.subjects.Has(fmt.Sprintf("role:%s", role)) {
					return true
				}
			}
		}
	}

	return false
}
