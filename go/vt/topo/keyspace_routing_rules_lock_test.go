/*
Copyright 2024 The Vitess Authors.

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

package topo_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
)

// TestKeyspaceRoutingRulesLock tests that the lock is acquired and released correctly.
func TestKeyspaceRoutingRulesLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()
	conn := ts.GetGlobalCell()
	require.NotNil(t, conn)
	now := time.Now()
	waitTime := 5 * time.Second

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := topotools.UpdateKeyspaceRoutingRulesLocked(ctx, ts, "ApplyKeyspaceRoutingRules",
			func(ctx context.Context) error {
				time.Sleep(waitTime)
				return nil
			})
		require.NoError(t, err)
	}()

	// This will wait for the lock until the previous holder releases it.
	err := topotools.UpdateKeyspaceRoutingRulesLocked(ctx, ts, "ApplyKeyspaceRoutingRules",
		func(ctx context.Context) error {
			return nil
		})
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(now), waitTime)
	wg.Wait()
}
