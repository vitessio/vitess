/*
Copyright 2023 The Vitess Authors.

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

package topotests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestCreateKeyspace(t *testing.T) {
	ts := memorytopo.NewServer("zone1")
	ctx := context.Background()

	t.Run("valid name", func(t *testing.T) {
		err := ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
		require.NoError(t, err)
	})
	t.Run("invalid name", func(t *testing.T) {
		err := ts.CreateKeyspace(ctx, "no/slashes/allowed", &topodatapb.Keyspace{})
		assert.Error(t, err)
		assert.Equal(t, vtrpc.Code_INVALID_ARGUMENT, vterrors.Code(err), "%+v", err)
	})
}

func TestGetKeyspace(t *testing.T) {
	ts := memorytopo.NewServer("zone1")
	ctx := context.Background()

	t.Run("valid name", func(t *testing.T) {
		// First, create the keyspace.
		err := ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
		require.NoError(t, err)

		// Now, get it.
		ks, err := ts.GetKeyspace(ctx, "ks")
		require.NoError(t, err)
		assert.NotNil(t, ks)
	})

	t.Run("invalid name", func(t *testing.T) {
		// We can't create the keyspace (because we can't create a keyspace
		// with an invalid name), so we'll validate the error we get is *not*
		// NOT_FOUND.
		ks, err := ts.GetKeyspace(ctx, "no/slashes/allowed")
		assert.Error(t, err)
		assert.Equal(t, vtrpc.Code_INVALID_ARGUMENT, vterrors.Code(err), "%+v", err)
		assert.Nil(t, ks)
	})
}
