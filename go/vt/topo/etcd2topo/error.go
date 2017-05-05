/*
Copyright 2017 Google Inc.

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

package etcd2topo

import (
	"errors"

	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"

	"github.com/youtube/vitess/go/vt/topo"
)

// Errors specific to this package.
var (
	// ErrBadResponse is returned from this package if the response from the etcd
	// server does not contain the data that the API promises. The etcd client
	// unmarshals JSON from the server into a Response struct that uses pointers,
	// so we need to check for nil pointers, or else a misbehaving etcd could
	// cause us to panic.
	ErrBadResponse = errors.New("etcd request returned success, but response is missing required data")
)

// convertError converts an etcd error into a topo error. All errors
// are either application-level errors, or context errors.
func convertError(err error) error {
	switch typeErr := err.(type) {
	case rpctypes.EtcdError:
		switch typeErr.Code() {
		case codes.NotFound:
			return topo.ErrNoNode
		}
	default:
		switch err {
		case context.Canceled:
			return topo.ErrInterrupted
		case context.DeadlineExceeded:
			return topo.ErrTimeout
		}
	}
	return err
}
