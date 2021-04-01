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

package testutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// SrvKeyspace groups a topodatapb.SrvKeyspace together with a keyspace and
// cell.
type SrvKeyspace struct {
	Keyspace    string
	Cell        string
	SrvKeyspace *topodatapb.SrvKeyspace
}

// AddSrvKeyspaces adds one or more SrvKeyspace objects to the topology. It
// fails the calling test if any of the objects fail to update.
func AddSrvKeyspaces(t *testing.T, ts *topo.Server, srvKeyspaces ...*SrvKeyspace) {
	t.Helper()

	ctx := context.Background()

	for _, sk := range srvKeyspaces {
		err := ts.UpdateSrvKeyspace(ctx, sk.Cell, sk.Keyspace, sk.SrvKeyspace)
		require.NoError(t, err, "UpdateSrvKeyspace(cell = %v, keyspace = %v, srv_keyspace = %v", sk.Cell, sk.Keyspace, sk.SrvKeyspace)
	}
}
