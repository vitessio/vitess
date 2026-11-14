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

package consultopo

import (
	"context"
	"errors"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
)

// TestConvertError verifies that convertError maps an error to a topo error only when the ctx of the call is
// done, and otherwise returns the error from the Go HTTP client unchanged, with its request URL.
func TestConvertError(t *testing.T) {
	const requestURL = "http://127.0.0.1:8500/v1/kv/root/file"
	wrap := func(err error) error {
		return &url.Error{Op: "Get", URL: requestURL, Err: err}
	}
	refused := wrap(&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")})
	dialTimeout := wrap(dialTimeoutError(t))
	// The trap that convertError must not fall into: a dial timeout matches context.DeadlineExceeded.
	require.ErrorIs(t, dialTimeout, context.DeadlineExceeded)

	liveCtx := t.Context()
	canceledCtx, cancel := context.WithCancel(liveCtx)
	cancel()
	expiredCtx, cancel := context.WithDeadline(liveCtx, time.Now())
	t.Cleanup(cancel)

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		// want is the topo error type expected, or -1 when the error must come back unchanged.
		want int
	}{{
		name: "live ctx keeps a refused connection",
		ctx:  liveCtx,
		err:  refused,
		want: -1,
	}, {
		name: "live ctx keeps a dial timeout",
		ctx:  liveCtx,
		err:  dialTimeout,
		want: -1,
	}, {
		name: "canceled ctx",
		ctx:  canceledCtx,
		err:  wrap(context.Canceled),
		want: int(topo.Interrupted),
	}, {
		name: "expired ctx",
		ctx:  expiredCtx,
		err:  wrap(context.DeadlineExceeded),
		want: int(topo.Timeout),
	}, {
		name: "expired ctx with a dial timeout",
		ctx:  expiredCtx,
		err:  dialTimeout,
		want: int(topo.Timeout),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertError(tt.ctx, tt.err, "file")
			if tt.want < 0 {
				assert.Same(t, tt.err, got, "the error was not returned unchanged")
				assert.ErrorContains(t, got, requestURL)
				return
			}
			assert.True(t, topo.IsErrType(got, topo.ErrorCode(tt.want)), "expected a topo error of code %v, got %v", tt.want, got)
		})
	}
}
