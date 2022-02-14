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
