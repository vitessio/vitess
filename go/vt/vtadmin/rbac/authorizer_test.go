package rbac

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsAuthorized(t *testing.T) {
	t.Parallel()

	authz, err := NewAuthorizer(&Config{
		Rules: []*struct {
			Resource string
			Actions  []string
			Subjects []string
			Clusters []string
		}{
			{
				Resource: "*",
				Actions:  []string{string(GetAction)},
				Subjects: []string{"user:testuser"},
				Clusters: []string{"*"},
			},
			{
				Resource: string(KeyspaceResource),
				Actions:  []string{"*"},
				Subjects: []string{"role:testrole"},
				Clusters: []string{"c1"},
			},
			{
				Resource: string(TabletResource),
				Actions:  []string{"*"},
				Subjects: []string{"*"},
				Clusters: []string{"*"},
			},
		},
	})
	require.NoError(t, err)

	tests := []struct {
		name         string
		actor        *Actor
		clusterID    string
		resource     Resource
		action       Action
		isAuthorized bool
	}{
		{
			name: "resource wildcard with user rule",
			actor: &Actor{
				Name: "testuser",
			},
			clusterID:    "c2",
			resource:     SchemaResource,
			action:       GetAction,
			isAuthorized: true,
		},
		{
			name: "resource rule with role rule",
			actor: &Actor{
				Name:  "someuser",
				Roles: []string{"testrole"},
			},
			clusterID:    "c1",
			resource:     KeyspaceResource,
			action:       GetAction,
			isAuthorized: true,
		},
		{
			name:         "nil actor",
			actor:        nil,
			clusterID:    "c1",
			resource:     KeyspaceResource,
			action:       GetAction,
			isAuthorized: false,
		},
		{
			name:         "wildcard subject",
			actor:        nil,
			clusterID:    "*",
			resource:     TabletResource,
			action:       GetAction,
			isAuthorized: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := NewContext(context.Background(), tt.actor)
			got := authz.IsAuthorized(ctx, tt.clusterID, tt.resource, tt.action)

			assert.Equal(t, tt.isAuthorized, got)
		})
	}
}
