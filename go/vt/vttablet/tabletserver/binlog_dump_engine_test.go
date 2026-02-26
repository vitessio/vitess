/*
Copyright 2025 The Vitess Authors.

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

package tabletserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBinlogDumpEngine_OpenClose(t *testing.T) {
	e := NewBinlogDumpEngine()
	require.False(t, e.IsOpen())

	e.Open()
	require.True(t, e.IsOpen())

	e.Close()
	require.False(t, e.IsOpen())
}

func TestBinlogDumpEngine_RejectWhenClosed(t *testing.T) {
	e := NewBinlogDumpEngine()

	// Not opened - should reject
	_, _, err := e.Register(context.Background())
	require.Error(t, err)

	require.Contains(t, err.Error(), "closed")
}

func TestBinlogDumpEngine_CloseCancelsContexts(t *testing.T) {
	e := NewBinlogDumpEngine()
	e.Open()

	ctx, idx, err := e.Register(context.Background())
	require.NoError(t, err)

	// Context should not be cancelled yet
	require.NoError(t, ctx.Err())

	// Unregister in background so Close can complete
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		e.Unregister(idx)
		close(done)
	}()

	e.Close()

	// Context should be cancelled now
	<-done
	require.Error(t, ctx.Err())
}

func TestBinlogDumpEngine_CloseWaitsForStreams(t *testing.T) {
	e := NewBinlogDumpEngine()
	e.Open()

	_, idx, err := e.Register(context.Background())
	require.NoError(t, err)

	closeDone := make(chan struct{})
	go func() {
		e.Close()
		close(closeDone)
	}()

	// Close should be blocked because stream is still registered
	select {
	case <-closeDone:
		t.Fatal("Close should block until streams are unregistered")
	default:
	}

	// Unregister the stream
	e.Unregister(idx)

	// Now Close should complete
	require.Eventually(t, func() bool {
		select {
		case <-closeDone:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)
}

func TestBinlogDumpEngine_RejectAfterClose(t *testing.T) {
	e := NewBinlogDumpEngine()
	e.Open()
	e.Close()

	_, _, err := e.Register(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "closed")
}
