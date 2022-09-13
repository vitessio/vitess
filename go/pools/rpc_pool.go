/*
Copyright 2021 The Vitess Authors.

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

package pools

import (
	"context"
	"time"

	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// RPCPool is a specialized version of the ResourcePool, for bounding concurrent
// access to making RPC calls.
//
// Whether you use this, or a sync2.Semaphore to gate RPCs (or shared access to
// any shared resource) depends on what timeout semantics you need. A sync2.Semaphore
// takes a global timeout, which applies to calls to Acquire(); if you need to
// respect a context's deadline, you can call AcquireContext(context.Context),
// but that ignores the global timeout. Conversely, an RPCPool provides only
// one method of acquisition, Acquire(context.Context), which always uses the
// lower of the pool-global timeout or the context deadline.
type RPCPool struct {
	rp          IResourcePool
	waitTimeout time.Duration
}

// NewRPCPool returns an RPCPool with the given size and wait timeout. A zero
// timeout will be ignored on calls to Acquire, and only the context deadline
// will be used. If a logWait function is provided, it will be called whenever
// a call to Acquire has to wait for a resource, but only after a successful
// wait (meaning if we hit a timeout before a resource becomes available, it
// will not be called).
func NewRPCPool(size int, waitTimeout time.Duration, logWait func(time.Time)) *RPCPool {
	return &RPCPool{
		rp:          NewResourcePool(rpcResourceFactory, size, size, 0, logWait, nil, 0),
		waitTimeout: waitTimeout,
	}
}

// Acquire acquires one slot in the RPCPool. If a slot is not immediately
// available, it will block until one becomes available or until a timeout is
// reached. The lower of the context deadline and the pool's waitTimeout will
// be used as the timeout, except when the pool's waitTimeout is zero, in which
// case the context deadline will always serve as the overall timeout.
//
// It returns nil on successful acquisition, and an error if a timeout occurred
// before a slot became available.
//
// Note: For every successful call to Acquire, the caller must make a
// corresponding call to Release.
func (pool *RPCPool) Acquire(ctx context.Context) error {
	if pool.waitTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, pool.waitTimeout)
		defer cancel()
	}

	_, err := pool.rp.Get(ctx, nil)
	return err
}

// Release frees a slot in the pool. It must only be called after a successful
// call to Acquire.
func (pool *RPCPool) Release() { pool.rp.Put(rpc) }

// Close empties the pool, preventing further Acquire calls from succeeding.
// It waits for all slots to be freed via Release.
func (pool *RPCPool) Close() { pool.rp.Close() }

func (pool *RPCPool) StatsJSON() string { return pool.rp.StatsJSON() }

type _rpc struct{}

var rpc = &_rpc{}

// Close implements Resource for _rpc.
func (*_rpc) Close() {}

// ApplySetting implements Resource for _rpc.
func (r *_rpc) ApplySetting(context.Context, *Setting) error {
	// should be unreachable
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG]: _rpc does not support ApplySetting")
}

func (r *_rpc) IsSettingApplied() bool {
	return false
}

func (r *_rpc) IsSameSetting(string) bool {
	return true
}

func (r *_rpc) ResetSetting(context.Context) error {
	// should be unreachable
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG]: _rpc does not support ResetSetting")
}

// we only ever return the same rpc pointer. it's used as a sentinel and is
// only used internally so using the same one over and over doesn't matter.
func rpcResourceFactory(ctx context.Context) (Resource, error) { return rpc, nil }
