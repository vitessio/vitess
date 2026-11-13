/*
Copyright 2019 The Vitess Authors.

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

	"vitess.io/vitess/go/vt/topo"
)

// Errors specific to this package.
var (
	// ErrBadResponse is returned from this package if the
	// response from the consul server does not contain the data
	// that the API promises. The consul client unmarshals JSON
	// from the server into a Response struct that uses pointers,
	// so we need to check for nil pointers, or else a misbehaving
	// consul could cause us to panic.
	ErrBadResponse = errors.New("consul request returned success, but response is missing required data")
)

// convertError converts the error of a Consul call made with ctx into a
// topo error when ctx is done, since the call then failed because of the
// ctx. Any other error is returned unchanged, so that errors from the Go
// HTTP client keep their request URL. The error itself can't tell: a dial
// timeout also matches context.DeadlineExceeded, while the caller's ctx may
// have no deadline at all.
func convertError(ctx context.Context, err error, nodePath string) error {
	switch ctx.Err() {
	case context.Canceled:
		return topo.NewError(topo.Interrupted, nodePath)
	case context.DeadlineExceeded:
		return topo.NewError(topo.Timeout, nodePath)
	}

	return err
}
