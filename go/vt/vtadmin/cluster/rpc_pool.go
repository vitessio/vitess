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

package cluster

import (
	"context"
	"time"

	"vitess.io/vitess/go/pools"
)

// RPCPool defines the interface used by Cluster's to pool concurrent outbound
// RPCs.
type RPCPool interface {
	Do(ctx context.Context, f func() error) error
}

// NewRPCPool returns a new RPCPool. If maxConcurrentRPCs is non-positive, the
// RPCPool is unbounded, and waitTimeout is irrelevant.
func NewRPCPool(maxConcurrentRPCs int, waitTimeout time.Duration) RPCPool {
	if maxConcurrentRPCs <= 0 {
		return &unboundedPool{}
	}

	return &boundedPool{
		rp:          pools.NewResourcePool(pools.NewEmptyResource, maxConcurrentRPCs, maxConcurrentRPCs, 0, maxConcurrentRPCs, nil),
		waitTimeout: waitTimeout,
	}
}

type unboundedPool struct{}

func (pool *unboundedPool) Do(ctx context.Context, f func() error) error { return f() }

type boundedPool struct {
	rp          *pools.ResourcePool
	waitTimeout time.Duration
}

func (pool *boundedPool) Do(ctx context.Context, f func() error) error {
	ctx, cancel := context.WithTimeout(ctx, pool.waitTimeout)
	defer cancel()

	return pool.rp.Do(ctx, func(resource pools.Resource) (pools.Resource, error) {
		return resource, f()
	})
}
