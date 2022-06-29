/*
Copyright 2022 The Vitess Authors.

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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLastError(t *testing.T) {
	le := newLastError("test", 100*time.Millisecond)

	t.Run("long running error", func(t *testing.T) {
		err1 := fmt.Errorf("test1")
		le.record(err1)
		require.True(t, le.shouldRetry())
		time.Sleep(150 * time.Millisecond)
		require.False(t, le.shouldRetry())
	})

	t.Run("new long running error", func(t *testing.T) {
		err2 := fmt.Errorf("test2")
		le.record(err2)
		require.True(t, le.shouldRetry())
		for i := 1; i < 10; i++ {
			le.record(err2)
		}
		require.True(t, le.shouldRetry())
		time.Sleep(150 * time.Millisecond)
		le.record(err2)
		require.False(t, le.shouldRetry())
	})

	t.Run("no error", func(t *testing.T) {
		le.record(nil)
		require.True(t, le.shouldRetry())
	})
}
