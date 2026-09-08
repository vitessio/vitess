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

package ioutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type hangCloser struct {
	hang bool
	err  error
}

func (c hangCloser) Close() error {
	if c.hang {
		ch := make(chan bool)
		ch <- true // hang forever
	}
	return c.err
}

func TestTimeoutCloser(t *testing.T) {
	ctx := t.Context()
	{
		closer := NewTimeoutCloser(ctx, &hangCloser{hang: false}, time.Second)
		err := closer.Close()
		require.NoError(t, err)
	}
	{
		closer := NewTimeoutCloser(ctx, &hangCloser{hang: true}, time.Second)
		err := closer.Close()
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.ErrorIs(t, err, ErrCloseAbandoned)
	}
}

// TestTimeoutCloserAbandoned pins what ErrCloseAbandoned means: it marks a
// Close that the TimeoutCloser stopped waiting for, whether because of its
// own timeout or because the context was canceled, and never a Close that
// returned on its own, even one that failed with a context error.
func TestTimeoutCloserAbandoned(t *testing.T) {
	{
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		closer := NewTimeoutCloser(ctx, &hangCloser{hang: true}, time.Minute)
		err := closer.Close()
		require.ErrorIs(t, err, ErrCloseAbandoned)
		require.ErrorIs(t, err, context.Canceled)
	}
	{
		closer := NewTimeoutCloser(t.Context(), &hangCloser{err: context.DeadlineExceeded}, time.Minute)
		err := closer.Close()
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NotErrorIs(t, err, ErrCloseAbandoned, "a Close that returned is not abandoned")
	}
}
