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

// Package buffer provides a buffer for PRIMARY traffic during failovers.
//
// Instead of returning an error to the application (when the vttablet primary
// becomes unavailable), the buffer will automatically retry buffered requests
// after the end of the failover was detected.
//
// Buffering (stalling) requests will increase the number of requests in flight
// within vtgate and at upstream layers. Therefore, it is important to limit
// the size of the buffer and the buffering duration (window) per request.
// See the file flags.go for the available configuration and its defaults.
package buffer

import (
	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	ShardMissingError    = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "destination shard is missing after a resharding operation")
	bufferFullError      = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "primary buffer is full")
	entryEvictedError    = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "buffer full: request evicted for newer request")
	contextCanceledError = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "context was canceled before failover finished")
)

// bufferMode specifies how the buffer is configured for a given shard.
type bufferMode int

const (
	// bufferDisabled will let all requests pass through and do nothing.
	bufferDisabled bufferMode = iota
	// bufferEnabled means all requests should be buffered.
	bufferEnabled
	// bufferDryRun will track the failover, but not actually buffer requests.
	bufferDryRun
)

type Buffer interface {
	WaitForFailoverEnd(ctx context.Context, keyspace, shard string, err error) (RetryDoneFunc, error)
	Shutdown()
}

// RetryDoneFunc will be returned for each buffered request and must be called
// after the buffered request was retried.
// Without this signal, the buffer would not know how many buffered requests are
// currently retried.
type RetryDoneFunc context.CancelFunc

// CausedByFailover returns true if "err" was supposedly caused by a failover.
// To simplify things, we've merged the detection for different MySQL flavors
// in one function. Supported flavors: MariaDB, MySQL, Google internal.
func CausedByFailover(err error) bool {
	log.V(2).Infof("Checking error (type: %T) if it is caused by a failover. err: %v", err, err)
	return vterrors.Code(err) == vtrpcpb.Code_CLUSTER_EVENT
}
