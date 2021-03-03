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

package etcd2topo

import (
	"errors"

	"context"

	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"vitess.io/vitess/go/vt/topo"
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
func convertError(err error, nodePath string) error {
	if err == nil {
		return nil
	}

	if typeErr, ok := err.(rpctypes.EtcdError); ok {
		switch typeErr.Code() {
		case codes.NotFound:
			return topo.NewError(topo.NoNode, nodePath)
		case codes.Unavailable, codes.DeadlineExceeded:
			// The etcd2 client library may return this error:
			// grpc.Errorf(codes.Unavailable,
			// "etcdserver: request timed out") which seems to be
			// misclassified, it should be using
			// codes.DeadlineExceeded. All timeouts errors
			// seem to be using the codes.Unavailable
			// category. So changing all of them to ErrTimeout.
			// The other reasons for codes.Unavailable are when
			// etcd master election is failing, so timeout
			// also sounds reasonable there.
			return topo.NewError(topo.Timeout, nodePath)
		}
		return err
	}

	if s, ok := status.FromError(err); ok {
		// This is a gRPC error.
		switch s.Code() {
		case codes.NotFound:
			return topo.NewError(topo.NoNode, nodePath)
		case codes.Canceled:
			return topo.NewError(topo.Interrupted, nodePath)
		case codes.DeadlineExceeded:
			return topo.NewError(topo.Timeout, nodePath)
		default:
			return err
		}
	}

	switch err {
	case context.Canceled:
		return topo.NewError(topo.Interrupted, nodePath)
	case context.DeadlineExceeded:
		return topo.NewError(topo.Timeout, nodePath)
	default:
		return err
	}
}
