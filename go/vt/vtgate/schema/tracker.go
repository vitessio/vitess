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

package schema

import (
	"context"
	"sync"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// Tracker contains the required fields to perform schema tracking.
type Tracker struct {
	ch     chan *discovery.TabletHealth
	cancel context.CancelFunc

	mu       sync.Mutex
	tableMap map[string][]vindexes.Column
}

// NewTracker creates the tracker object.
func NewTracker(ch chan *discovery.TabletHealth) *Tracker {
	return &Tracker{ch: ch}
}

// Start starts the schema tracking.
func (t *Tracker) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	t.cancel = cancel
	go func(ctx context.Context, t *Tracker) {
		for {
			select {
			case <-t.ch:
			case <-ctx.Done():
				close(t.ch)
			}
		}
	}(ctx, t)
}

// Stop stops the schema tracking
func (t *Tracker) Stop() {
	t.cancel()
}

// GetColumns returns the column list for table in the given keyspace.
func (t *Tracker) GetColumns(ks string, tbl string) []vindexes.Column {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := ks + "." + tbl
	return t.tableMap[key]
}
