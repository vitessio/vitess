/*
Copyright 2026 The Vitess Authors.

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

package faketopo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestDelete(t *testing.T) {
	for _, tc := range []struct {
		name      string
		versioned bool
	}{
		{name: "unversioned"},
		{name: "versioned", versioned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			conn := NewFakeConnection()
			const filePath = "keyspaces/ks/shards/0/non_managed_tablets/cell1-0000000002"
			assert.True(t, topo.IsErrType(conn.Delete(ctx, filePath, nil), topo.NoNode))
			version, err := conn.Create(ctx, filePath, []byte("marker"))
			require.NoError(t, err)
			assert.True(t, topo.IsErrType(conn.Delete(ctx, filePath, memorytopo.NodeVersion(2)), topo.BadVersion))
			contents, _, err := conn.Get(ctx, filePath)
			require.NoError(t, err)
			assert.Equal(t, []byte("marker"), contents)
			if !tc.versioned {
				version = nil
			}
			require.NoError(t, conn.Delete(ctx, filePath, version))
			_, _, err = conn.Get(ctx, filePath)
			assert.True(t, topo.IsErrType(err, topo.NoNode))
			assert.True(t, topo.IsErrType(conn.Delete(ctx, filePath, nil), topo.NoNode))
		})
	}
}
