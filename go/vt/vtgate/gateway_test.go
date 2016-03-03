// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"reflect"
	"testing"
	"time"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestGatwayEndPointStatusAggregator(t *testing.T) {
	aggr := &GatewayEndPointStatusAggregator{
		Keyspace:   "k",
		Shard:      "s",
		TabletType: topodatapb.TabletType_REPLICA,
		Name:       "n",
		Addr:       "a",
		qiChan:     make(chan *queryInfo, 10000),
	}
	t.Logf("aggr = GatwayEndPointStatusAggregator{k, s, replica, n, a}")
	aggr.UpdateQueryInfo("", topodatapb.TabletType_REPLICA, 10*time.Millisecond, false)
	aggr.processQueryInfo(<-aggr.qiChan)
	t.Logf("aggr.UpdateQueryInfo(, replica, 10ms, false)")
	aggr.resetNextSlot()
	t.Logf("aggr.resetNextSlot()")
	aggr.UpdateQueryInfo("", topodatapb.TabletType_REPLICA, 8*time.Millisecond, false)
	aggr.processQueryInfo(<-aggr.qiChan)
	t.Logf("aggr.UpdateQueryInfo(, replica, 8ms, false)")
	aggr.UpdateQueryInfo("", topodatapb.TabletType_REPLICA, 3*time.Millisecond, true)
	aggr.processQueryInfo(<-aggr.qiChan)
	t.Logf("aggr.UpdateQueryInfo(, replica, 3ms, true)")
	want := &GatewayEndPointCacheStatus{
		Keyspace:   "k",
		Shard:      "s",
		Name:       "n",
		TabletType: topodatapb.TabletType_REPLICA,
		Addr:       "a",
		QueryCount: 3,
		QueryError: 1,
		QPS:        0,
		AvgLatency: 7,
	}
	got := aggr.GetCacheStatus()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("aggr.GetCacheStatus() = %+v, want %+v", got, want)
	}
	// reset values in idx=0
	for i := 0; i < 59; i++ {
		aggr.resetNextSlot()
	}
	t.Logf("59 aggr.resetNextSlot()")
	aggr.UpdateQueryInfo("b", topodatapb.TabletType_MASTER, 9*time.Millisecond, false)
	aggr.processQueryInfo(<-aggr.qiChan)
	t.Logf("aggr.UpdateQueryInfo(b, master, 9ms, false)")
	aggr.UpdateQueryInfo("", topodatapb.TabletType_MASTER, 6*time.Millisecond, true)
	aggr.processQueryInfo(<-aggr.qiChan)
	t.Logf("aggr.UpdateQueryInfo(, master, 6ms, true)")
	want = &GatewayEndPointCacheStatus{
		Keyspace:   "k",
		Shard:      "s",
		Name:       "n",
		TabletType: topodatapb.TabletType_MASTER,
		Addr:       "b",
		QueryCount: 2,
		QueryError: 1,
		QPS:        0,
		AvgLatency: 7.5,
	}
	got = aggr.GetCacheStatus()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("aggr.GetCacheStatus() = %+v, want %+v", got, want)
	}
}
