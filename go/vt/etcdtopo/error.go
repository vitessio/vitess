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

package etcdtopo

import (
	"errors"

	"golang.org/x/net/context"

	"github.com/coreos/go-etcd/etcd"
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

// Error codes returned by etcd:
// https://github.com/coreos/etcd/blob/v0.4.6/Documentation/errorcode.md
const (
	EcodeKeyNotFound    = 100
	EcodeTestFailed     = 101
	EcodeNotFile        = 102
	EcodeNoMorePeer     = 103
	EcodeNotDir         = 104
	EcodeNodeExist      = 105
	EcodeKeyIsPreserved = 106
	EcodeRootROnly      = 107
	EcodeDirNotEmpty    = 108

	EcodeValueRequired     = 200
	EcodePrevValueRequired = 201
	EcodeTTLNaN            = 202
	EcodeIndexNaN          = 203

	EcodeRaftInternal = 300
	EcodeLeaderElect  = 301

	EcodeWatcherCleared    = 400
	EcodeEventIndexCleared = 401
)

// convertError converts etcd-specific errors to corresponding topo errors, if
// they exist, and passes others through. It also converts context errors to
// topo package equivalents.
func convertError(err error) error {
	switch typeErr := err.(type) {
	case *etcd.EtcdError:
		switch typeErr.ErrorCode {
		case EcodeTestFailed:
			return topo.ErrBadVersion
		case EcodeKeyNotFound:
			return topo.ErrNoNode
		case EcodeNodeExist:
			return topo.ErrNodeExists
		case EcodeDirNotEmpty:
			return topo.ErrNotEmpty
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
