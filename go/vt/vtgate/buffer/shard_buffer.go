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

package buffer

import (
	"context"
	"time"
)

// bufferState represents the different states a shardBuffer object can be in.
type bufferState string

const (
	// stateIdle means no failover is currently in progress.
	stateIdle bufferState = "IDLE"
	// stateBuffering is the phase when a failover is in progress.
	stateBuffering bufferState = "BUFFERING"
	// stateDraining is the phase when a failover ended and the queue is drained.
	stateDraining bufferState = "DRAINING"
)

// entry is created per buffered request.
type entry struct {
	// done will be closed by shardBuffer when the failover is over and the
	// request can be retried.
	// Any Go routine closing this channel must also remove the entry from the
	// ShardBuffer queue such that nobody else tries to close it.
	done chan struct{}

	// deadline is the time when the entry is out of the buffering window and it
	// must be canceled.
	deadline time.Time

	// err is set if the buffering failed e.g. when the entry was evicted.
	err error

	// bufferCtx wraps the request ctx and is used to track the retry of a
	// request during the drain phase. Once the retry is done, the caller
	// must cancel this context (by calling bufferCancel).
	bufferCtx    context.Context
	bufferCancel func()
}
