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

package vstreamer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestWrapErrorPreservesCode pins the vterrors.Wrapf change: the vplayer
// treats FAILED_PRECONDITION stream errors as terminal via isUnrecoverableError,
// which only sees the code if wrapError does not flatten the error to text.
func TestWrapErrorPreservesCode(t *testing.T) {
	vse := &Engine{
		vstreamersEndedWithErrors: stats.NewCounter("TestWrapErrorVStreamersEndedWithErrors", "test"),
		errorCounts:               stats.NewCountersWithSingleLabel("TestWrapErrorVStreamerErrors", "test", "type"),
	}
	pos, err := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	require.NoError(t, err)

	t.Run("nil", func(t *testing.T) {
		assert.NoError(t, wrapError(nil, pos, vse))
	})

	t.Run("FAILED_PRECONDITION", func(t *testing.T) {
		orig := vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "missing a needed value")
		wrapped := wrapError(orig, pos, vse)
		require.Error(t, wrapped)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(wrapped))
		require.ErrorContains(t, wrapped, "missing a needed value")
		require.ErrorContains(t, wrapped, "stream (at source tablet) error")
	})

	t.Run("INTERNAL", func(t *testing.T) {
		orig := vterrors.Errorf(vtrpcpb.Code_INTERNAL, "internal boom")
		wrapped := wrapError(orig, pos, vse)
		require.Error(t, wrapped)
		assert.Equal(t, vtrpcpb.Code_INTERNAL, vterrors.Code(wrapped))
	})
}
