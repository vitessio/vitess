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

package vreplication

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestFormatTaskError(t *testing.T) {
	root := errors.New("failed inserting rows: EOF")
	dep := &dependentBatchFailure{msg: "received result is not complete"}
	cancelDep := &dependentBatchFailure{msg: "context has expired", code: vtrpcpb.Code_CANCELED}

	t.Run("nil for empty input", func(t *testing.T) {
		assert.NoError(t, formatTaskError(nil))
		assert.NoError(t, formatTaskError([]error{}))
		assert.NoError(t, formatTaskError([]error{nil, nil}))
	})

	t.Run("root only surfaces the root cause", func(t *testing.T) {
		err := formatTaskError([]error{root})
		require.Error(t, err)
		require.ErrorContains(t, err, "task error")
		require.ErrorContains(t, err, "failed inserting rows: EOF")
		assert.NotContains(t, err.Error(), "batches failed waiting on")
	})

	t.Run("mixed root + dependent prefers the root and counts the dependents", func(t *testing.T) {
		err := formatTaskError([]error{root, dep, dep, dep})
		require.Error(t, err)
		require.ErrorContains(t, err, "failed inserting rows: EOF")
		require.ErrorContains(t, err, "+3 batches failed waiting on this to complete")
		assert.NotContains(t, err.Error(), "received result is not complete",
			"dependent-batch echo strings must not appear in the message")
	})

	t.Run("dependent only directs operators to earlier rows", func(t *testing.T) {
		err := formatTaskError([]error{dep, dep, dep, dep})
		require.Error(t, err)
		require.ErrorContains(t, err, "4 batches failed waiting on a concurrent insert worker's batch")
		require.ErrorContains(t, err, "original failure not captured this retry")
		assert.ErrorContains(t, err, "see earlier rows for the root cause")
	})

	t.Run("dependent only preserves Code_CANCELED for ctx.Done() case", func(t *testing.T) {
		// tryAdvance maps Code_CANCELED to vcopierCopyTaskCancel; the
		// ctx.Done() dependentBatchFailure must keep that code so existing
		// cancel detection downstream still works.
		err := formatTaskError([]error{cancelDep, cancelDep})
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_CANCELED, vterrors.Code(err))
	})

	t.Run("multiple distinct root causes are all surfaced", func(t *testing.T) {
		err := formatTaskError([]error{
			errors.New("failed inserting rows: EOF"),
			errors.New("error committing transaction: write conflict"),
			dep,
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "failed inserting rows: EOF")
		require.ErrorContains(t, err, "error committing transaction: write conflict")
		assert.ErrorContains(t, err, "+1 batches failed waiting on this to complete")
	})
}
