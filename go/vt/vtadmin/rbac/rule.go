package rbac

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/sets"
)

// Rule is a single rule governing access to a particular resource.
type Rule struct {
	clusters sets.String
	actions  sets.String
	subjects sets.String
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
