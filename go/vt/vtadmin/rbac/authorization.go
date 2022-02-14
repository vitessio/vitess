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
// 	authz, err := rbac.NewAuthorizer(&rbac.Config{
// 		Rules: []*struct {
// 			Resource string
// 			Actions  []string
// 			Subjects []string
// 			Clusters []string
// 		}{
// 			{
// 				Resource: "*",
// 				Actions:  []string{"*"},
// 				Subjects: []string{"*"},
// 				Clusters: []string{"*"},
// 			},
// 		},
// 	})
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
