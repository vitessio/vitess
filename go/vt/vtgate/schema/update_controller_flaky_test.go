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
				updatedTables = th.Stats.TableSchemaChanged
				return !test.updateFail
			}
			signal := func() {
				signalNb++
			}
			updateCont := updateController{
				update:       update,
				signal:       signal,
				consumeDelay: 5 * time.Millisecond,
				reloadKeyspace: func(th *discovery.TabletHealth) error {
					initNb++
					var err error
					if test.initFail {
						err = fmt.Errorf("error")
					}
					return err
				},
				loaded: !test.init,
			}

			for _, in := range test.inputs {
				target := &querypb.Target{
					Keyspace:   "ks",
					Shard:      in.shard,
					TabletType: topodatapb.TabletType_PRIMARY,
				}
				tablet := &topodatapb.Tablet{
					Keyspace: target.Keyspace,
					Shard:    target.Shard,
					Type:     target.TabletType,
				}
				d := &discovery.TabletHealth{
					Tablet:  tablet,
					Target:  target,
					Serving: true,
					Stats:   &querypb.RealtimeStats{TableSchemaChanged: in.tablesUpdates},
				}
				if test.delay > 0 {
					time.Sleep(test.delay)
				}
				updateCont.add(d)
			}

			for {
				updateCont.mu.Lock()
				done := updateCont.queue == nil
				updateCont.mu.Unlock()
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

// TestViewUpdate tests the update of the view handled as expected.
func TestViewsUpdates(t *testing.T) {
	// receives a input from a shard and what views are updated.
	type input struct {
		shard       string
		viewUpdates []string
	}
	// test will receive multiple inputs and then find the consolidated view updates.
	// if able to receive results successfully, then signal is made to the consumer.
	type testCase struct {
		desc                         string
		updateViews                  []string
		signalExpected, initExpected int
		inputs                       []input
		delay                        time.Duration // this causes a delay between inputs
		init, initFail, updateFail   bool
	}
	tests := []testCase{{
		desc:           "received same view updates from shards",
		inputs:         []input{{shard: "-80", viewUpdates: []string{"a"}}, {shard: "80-", viewUpdates: []string{"a"}}},
		updateViews:    []string{"a"},
		signalExpected: 1,
	}, {
		desc:           "received different view updates from shards",
		inputs:         []input{{shard: "0", viewUpdates: []string{"a"}}, {shard: "0", viewUpdates: []string{"b"}}},
		updateViews:    []string{"a", "b"},
		signalExpected: 1,
	}, {
		desc:           "delay between inputs - different signals from each input",
		inputs:         []input{{shard: "0", viewUpdates: []string{"a"}}, {shard: "0", viewUpdates: []string{"b"}}},
		updateViews:    []string{"b"},
		signalExpected: 2,
		delay:          10 * time.Millisecond,
	}, {
		desc:   "no change - no signal",
		inputs: []input{{shard: "0"}, {shard: "0"}},
	}, {
		desc:           "initialization did not happen - full views load over only updated views",
		inputs:         []input{{shard: "-80", viewUpdates: []string{"a"}}, {shard: "80-", viewUpdates: []string{"a"}}},
		signalExpected: 1,
		initExpected:   1,
		init:           true,
	}, {
		desc:           "initialization did not happen - full view load failed - no signal",
		inputs:         []input{{shard: "-80", viewUpdates: []string{"a"}}, {shard: "80-", viewUpdates: []string{"a"}}},
		signalExpected: 0,
		initExpected:   1,
		init:           true,
		initFail:       true,
	}, {
		desc:           "updated views failed - no signal",
		inputs:         []input{{shard: "-80", viewUpdates: []string{"a"}}, {shard: "80-", viewUpdates: []string{"b"}}},
		updateViews:    []string{"a", "b"},
		signalExpected: 0,
		updateFail:     true,
	},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i+1), func(t *testing.T) {
			var signalNb, initNb int
			var updatedViews []string
			update := func(th *discovery.TabletHealth) bool {
				updatedViews = th.Stats.ViewSchemaChanged
				return !test.updateFail
			}
			signal := func() {
				signalNb++
			}
			updateCont := updateController{
				update:       update,
				signal:       signal,
				consumeDelay: 5 * time.Millisecond,
				reloadKeyspace: func(th *discovery.TabletHealth) error {
					initNb++
					var err error
					if test.initFail {
						err = fmt.Errorf("error")
					}
					return err
				},
				loaded: !test.init,
			}

			for _, in := range test.inputs {
				target := &querypb.Target{
					Keyspace:   "ks",
					Shard:      in.shard,
					TabletType: topodatapb.TabletType_PRIMARY,
				}
				tablet := &topodatapb.Tablet{
					Keyspace: target.Keyspace,
					Shard:    target.Shard,
					Type:     target.TabletType,
				}
				d := &discovery.TabletHealth{
					Tablet:  tablet,
					Target:  target,
					Serving: true,
					Stats:   &querypb.RealtimeStats{ViewSchemaChanged: in.viewUpdates},
				}
				if test.delay > 0 {
					time.Sleep(test.delay)
				}
				updateCont.add(d)
			}

			for {
				updateCont.mu.Lock()
				done := updateCont.queue == nil
				updateCont.mu.Unlock()
				if done {
					break
				}
			}

			assert.Equal(t, test.signalExpected, signalNb, "signal required")
			assert.Equal(t, test.initExpected, initNb, "init required")
			assert.Equal(t, test.updateViews, updatedViews, "views to update")
		})
	}
}
