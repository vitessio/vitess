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

// Package reparenttestutil contains utility functions for writing tests
package reparenttestutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
)

// SetKeyspaceDurability sets the durability policy of a given keyspace. This function should only be used in test code
// To make it explicit and prevent any future errors, we are taking a testing.T argument, even though we could have returned the error
func SetKeyspaceDurability(ctx context.Context, t *testing.T, ts *topo.Server, keyspace, durability string) {
	ctx, unlock, lockErr := ts.LockKeyspace(ctx, keyspace, "testutil.SetKeyspaceDurability")
	if lockErr != nil {
		return
	}

	var err error
	defer unlock(&err)

	ki, err := ts.GetKeyspace(ctx, keyspace)
	require.NoError(t, err)

	ki.DurabilityPolicy = durability

	err = ts.UpdateKeyspace(ctx, ki)
	require.NoError(t, err)
}
