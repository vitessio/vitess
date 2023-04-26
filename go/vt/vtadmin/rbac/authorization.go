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
	"context"
)

// Authorizer contains a set of rules that determine which actors may take which
// actions on which resources in which clusters.
type Authorizer struct {
	// keyed by resource name
	policies map[string][]*Rule
}

// NewAuthorizer returns a new Authorizer based on the given Config, which
// typically comes from a marshaled yaml/json/toml file.
//
// To get an authorizer that permits all access, including from unauthenticated
// actors, provide the following:
//
//	authz, err := rbac.NewAuthorizer(&rbac.Config{
//		Rules: []*struct {
//			Resource string
//			Actions  []string
//			Subjects []string
//			Clusters []string
//		}{
//			{
//				Resource: "*",
//				Actions:  []string{"*"},
//				Subjects: []string{"*"},
//				Clusters: []string{"*"},
//			},
//		},
//	})
func NewAuthorizer(cfg *Config) (*Authorizer, error) {
	if err := cfg.Reify(); err != nil {
		return nil, err
	}

	return &Authorizer{
		policies: cfg.cfg,
	}, nil
}

// IsAuthorized returns whether an Actor (from the context) is permitted to take
// the given action on the given resource in the given cluster.
func (authz *Authorizer) IsAuthorized(ctx context.Context, clusterID string, resource Resource, action Action) bool {
	actor, _ := FromContext(ctx) // nil is ok here, since rule.Allows handles it
	if p, ok := authz.policies["*"]; ok {
		// We have policies for the wildcard resource to check first
		for _, rule := range p {
			if rule.Allows(clusterID, action, actor) {
				return true
			}
		}
	}

	if p, ok := authz.policies[string(resource)]; ok {
		for _, rule := range p {
			if rule.Allows(clusterID, action, actor) {
				return true
			}
		}
	}

	return false
}
