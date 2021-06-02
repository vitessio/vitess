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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestMultipleUpdatesFromDifferentShards(t *testing.T) {
	type input struct {
		shard         string
		tablesUpdates []string
	}
	type testCase struct {
		updateTables                 []string
		signalExpected, initExpected int
		inputs                       []input
		delay                        time.Duration
		init, initFail, updateFail   bool
	}
	tests := []testCase{{
		inputs: []input{{
			shard:         "-80",
			tablesUpdates: []string{"a"},
		}, {
			shard:         "80-",
			tablesUpdates: []string{"a"},
		}},
		updateTables:   []string{"a"},
		signalExpected: 1,
	}, {
		inputs: []input{{
			shard:         "0",
			tablesUpdates: []string{"a"},
		}, {
			shard:         "0",
			tablesUpdates: []string{"b"},
		}},
		updateTables:   []string{"a", "b"},
		signalExpected: 1,
	}, {
		inputs: []input{{
			shard:         "0",
			tablesUpdates: []string{"a"},
		}, {
			shard:         "0",
			tablesUpdates: []string{"b"},
		}},
		updateTables:   []string{"b"},
		signalExpected: 2,
		delay:          10 * time.Millisecond,
	}, {
		inputs: []input{{
			shard: "0",
		}, {
			shard: "0",
		}},
	}, {
		inputs: []input{{
			shard:         "-80",
			tablesUpdates: []string{"a"},
		}, {
			shard:         "80-",
			tablesUpdates: []string{"a"},
		}},
		signalExpected: 1,
		initExpected:   1,
		init:           true,
	}, {
		inputs: []input{{
			shard:         "-80",
			tablesUpdates: []string{"a"},
		}, {
			shard:         "80-",
			tablesUpdates: []string{"a"},
		}},
		signalExpected: 0,
		initExpected:   1,
		init:           true,
		initFail:       true,
	}, {
		inputs: []input{{
			shard:         "-80",
			tablesUpdates: []string{"a"},
		}, {
			shard:         "80-",
			tablesUpdates: []string{"b"},
		}},
		updateTables:   []string{"a", "b"},
		signalExpected: 0,
		updateFail:     true,
	},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i+1), func(t *testing.T) {
			var signalNb, initNb int
			var updatedTables []string
			update := func(th *discovery.TabletHealth) bool {
				updatedTables = th.TablesUpdated
				return !test.updateFail
			}
			signal := func() {
				signalNb++
			}
			kUpdate := updateController{
				update:       update,
				signal:       signal,
				consumeDelay: 5 * time.Millisecond,
			}

			if test.init {
				kUpdate.init = func(th *discovery.TabletHealth) bool {
					initNb++
					return !test.initFail
				}
			}

			for _, in := range test.inputs {
				target := &querypb.Target{
					Keyspace: "ks",
					Shard:    in.shard,
				}
				tablet := &topodatapb.Tablet{
					Keyspace: target.Keyspace,
					Shard:    target.Shard,
					Type:     target.TabletType,
				}
				d := &discovery.TabletHealth{
					Tablet:        tablet,
					Target:        target,
					Serving:       true,
					TablesUpdated: in.tablesUpdates,
				}
				if test.delay > 0 {
					time.Sleep(test.delay)
				}
				kUpdate.add(d)
			}

			for {
				kUpdate.mu.Lock()
				done := kUpdate.queue == nil
				kUpdate.mu.Unlock()
				if done {
					break
				}
			}

			assert.Equal(t, test.signalExpected, signalNb, "signal required")
			assert.Equal(t, test.initExpected, initNb, "init required")
			assert.Equal(t, test.updateTables, updatedTables, "tables to update")

		})
	}
}
