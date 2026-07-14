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

package grpcvtgateconn

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestDialDoesNotAccumulateOpts ensures the dialer returned by Dial does not
// append to the caller-provided options slice. The dialer is registered once
// and invoked for every new connection, so appending to the captured slice
// grows it by one option per dial and mutates the caller's backing array.
// See https://github.com/vitessio/vitess/issues/12068.
func TestDialDoesNotAccumulateOpts(t *testing.T) {
	// Extra capacity so a buggy append to the captured slice writes into
	// the caller's backing array instead of reallocating.
	callerOpts := make([]grpc.DialOption, 1, 8)
	callerOpts[0] = grpc.WithUserAgent("grpcvtgateconn-test")

	dialer := Dial(callerOpts...)
	for range 3 {
		conn, err := dialer(t.Context(), "localhost:0")
		require.NoError(t, err)
		conn.Close()
	}

	for i, opt := range callerOpts[len(callerOpts):cap(callerOpts)] {
		assert.Nil(t, opt, "dialer mutated the caller's options slice at index %d", len(callerOpts)+i)
	}
}

// TestDialConcurrent ensures concurrent invocations of a shared dialer do not
// race on the captured options slice. database/sql dials new pool connections
// from multiple goroutines; a race here tears dial option interface values
// and panics inside grpc when they are applied.
// See https://github.com/vitessio/vitess/issues/12067.
func TestDialConcurrent(t *testing.T) {
	dialer := Dial(grpc.WithUserAgent("grpcvtgateconn-test"))

	// Each goroutine writes its own index, so no synchronization is needed;
	// require lives in the test goroutine after Wait, where FailNow is valid.
	const n = 10
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			conn, err := dialer(t.Context(), "localhost:0")
			if err != nil {
				errs[i] = err
				return
			}
			conn.Close()
		})
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
}
