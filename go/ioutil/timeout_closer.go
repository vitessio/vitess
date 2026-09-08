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
	"errors"
	"fmt"
	"io"
	"time"
)

// ErrCloseAbandoned is returned by TimeoutCloser.Close, wrapped together
// with the context error, when the wrapped Close has not returned by the
// time the timeout expires or the context is canceled. The wrapped Close
// is still running at that point and may still be using whatever it was
// closing, so the caller must not treat those resources as released.
var ErrCloseAbandoned = errors.New("close abandoned, the wrapped Close has not returned")

// TimeoutCloser is an io.Closer that has a timeout for executing the Close() function.
type TimeoutCloser struct {
	ctx     context.Context
	closer  io.Closer
	timeout time.Duration
}

func NewTimeoutCloser(ctx context.Context, closer io.Closer, timeout time.Duration) *TimeoutCloser {
	return &TimeoutCloser{
		ctx:     ctx,
		closer:  closer,
		timeout: timeout,
	}
}

func (c *TimeoutCloser) Close() error {
	done := make(chan error)

	ctx, cancel := context.WithTimeout(c.ctx, c.timeout)
	defer cancel()

	go func() {
		defer close(done)
		select {
		case done <- c.closer.Close():
		case <-ctx.Done():
		}
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", ErrCloseAbandoned, ctx.Err())
	}
}
