/*
Copyright 2023 The Vitess Authors.

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

package srvtopo

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/stats"
)

// TestResilientQueryGetCurrentValueInitialization tests that the resilient query returns the correct results when it has been
// initialized.
func TestResilientQueryGetCurrentValueInitialization(t *testing.T) {
	// Create a basic query, which doesn't do anything other than return the same cell it got as an input.
	// The query however needs to simulate being slow, so we have a sleep in there.
	query := func(ctx context.Context, entry *queryEntry) (any, error) {
		time.Sleep(1 * time.Second)
		cell := entry.key.(cellName)
		return cell, nil
	}
	counts := stats.NewCountersWithSingleLabel("TestResilientQueryGetCurrentValue", "Test for resilient query", "type")

	// Create the resilient query
	rq := &resilientQuery{
		query:                query,
		counts:               counts,
		cacheRefreshInterval: 5 * time.Second,
		cacheTTL:             5 * time.Second,
		entries:              make(map[string]*queryEntry),
	}

	// Create a context and a cell.
	ctx := t.Context()
	cell := cellName("cell-1")

	// Hammer the resilient query with multiple get requests just as it is created.
	// We expect all of them to work.
	wg := sync.WaitGroup{}
	errs := make([]error, 10)
	ress := make([]any, 10)
	for i := range 10 {
		// To test with both stale and not-stale, we use the modulo of our index.
		stale := i%2 == 0
		wg.Go(func() {
			ress[i], errs[i] = rq.getCurrentValue(ctx, cell, stale)
		})
	}
	// Wait for the wait group to be empty, otherwise the test is marked a success before any of the go routines finish completion!
	wg.Wait()
	// Assert that we don't have any error and the value matches what we want.
	for i := range 10 {
		require.NoError(t, errs[i])
		assert.EqualValues(t, cell, ress[i])
	}
}
