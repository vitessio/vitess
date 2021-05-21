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
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"
)

func TestTracking(t *testing.T) {
	target := &querypb.Target{
		Keyspace:   "ks",
		Shard:      "-80",
		TabletType: topodatapb.TabletType_MASTER,
		Cell:       "aa",
	}
	tablet := &topodatapb.Tablet{}
	sbc := sandboxconn.NewSandboxConn(tablet)
	ch := make(chan *discovery.TabletHealth)
	tracker := NewTracker(ch)
	waiter := &testWaiter{}
	tracker.StartWithWaiter(waiter)
	defer tracker.Stop()
	result := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("a|b|c", "varchar|varchar|varchar"),
		"t|id|int",
		"t|name|varchar",
		"otherTbl|id|varchar",
	)
	sbc.SetResults([]*sqltypes.Result{result})

	tableName := "t"
	ch <- &discovery.TabletHealth{
		Conn:          sbc,
		Tablet:        tablet,
		Target:        target,
		Serving:       true,
		TablesUpdated: []string{tableName, "otherTbl"},
	}

	waiter.wait()

	assert.Contains(t, sbc.StringQueries(), "le query")
}

// this struct helps us test without time.Sleep
type testWaiter struct {
	wg sync2.AtomicBool
}

func (i *testWaiter) done() {
	i.wg.Set(true)
}
func (i *testWaiter) reset() {
	i.wg.Set(false)
}
func (i *testWaiter) wait() {
	for !i.wg.Get() {
		// busy loop until done
	}
}
