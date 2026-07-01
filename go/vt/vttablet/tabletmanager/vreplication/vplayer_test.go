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
)

func TestRunWithRecover(t *testing.T) {
	t.Run("panic is converted into an error", func(t *testing.T) {
		err := runWithRecover("wf1", "applyEvents", func() error {
			panic("boom")
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "panic in applyEvents")
		assert.ErrorContains(t, err, "boom")
	})

	t.Run("runtime panic (slice bounds out of range) is converted into an error", func(t *testing.T) {
		// Mirrors the #20360 shape: a runtime panic from indexing a too-short
		// slice inside the wrapped function should surface as a clean error,
		// not crash the test process.
		err := runWithRecover("wf2", "vstream", func() error {
			s := []int{}
			_ = s[0]
			return nil
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "panic in vstream")
		assert.ErrorContains(t, err, "index out of range")
	})

	t.Run("error returned by fn is passed through unchanged", func(t *testing.T) {
		sentinel := errors.New("real error")
		err := runWithRecover("wf3", "applyEvents", func() error {
			return sentinel
		})
		assert.ErrorIs(t, err, sentinel)
	})

	t.Run("nil return is preserved", func(t *testing.T) {
		err := runWithRecover("wf4", "applyEvents", func() error {
			return nil
		})
		assert.NoError(t, err)
	})
}
