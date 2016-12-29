// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
