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

package engine

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TestLockFailsClosedWhenKeyspaceIsUnavailable verifies that an advisory-lock
// plan reports its deterministic keyspace as unavailable rather than choosing a
// different lock namespace.
func TestLockFailsClosedWhenKeyspaceIsUnavailable(t *testing.T) {
	vcursor := &loggingVCursor{shardErr: errors.New("no local SrvKeyspace")}
	lock := &Lock{
		Keyspace:          &vindexes.Keyspace{Name: "lock_keyspace"},
		TargetDestination: key.DestinationKeyspaceID{0},
	}

	_, err := lock.TryExecute(t.Context(), vcursor, nil, false)
	require.ErrorContains(t, err, "advisory-lock keyspace lock_keyspace is unavailable")
}
