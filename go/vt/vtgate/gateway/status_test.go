/*
Copyright 2017 Google Inc.

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

package gateway

import (
	"bytes"
	"html/template"
	"reflect"
	"testing"
	"time"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestTabletStatusAggregator(t *testing.T) {
	aggr := &TabletStatusAggregator{
		Keyspace:   "k",
		Shard:      "s",
		TabletType: topodatapb.TabletType_REPLICA,
		Name:       "n",
		Addr:       "a",
	}
	t.Logf("aggr = TabletStatusAggregator{k, s, replica, n, a}")
	qi := &queryInfo{
		aggr:       aggr,
		addr:       "",
		tabletType: topodatapb.TabletType_REPLICA,
		elapsed:    10 * time.Millisecond,
		hasError:   false,
	}
	aggr.processQueryInfo(qi)
	t.Logf("aggr.processQueryInfo(, replica, 10ms, false)")
	aggr.resetNextSlot()
	t.Logf("aggr.resetNextSlot()")
	qi = &queryInfo{
		aggr:       aggr,
		addr:       "",
		tabletType: topodatapb.TabletType_REPLICA,
		elapsed:    8 * time.Millisecond,
		hasError:   false,
	}
	aggr.processQueryInfo(qi)
	t.Logf("aggr.processQueryInfo(, replica, 8ms, false)")
	qi = &queryInfo{
		aggr:       aggr,
		addr:       "",
		tabletType: topodatapb.TabletType_REPLICA,
		elapsed:    3 * time.Millisecond,
		hasError:   true,
	}
	aggr.processQueryInfo(qi)
	t.Logf("aggr.processQueryInfo(, replica, 3ms, true)")
	want := &TabletCacheStatus{
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
	qi = &queryInfo{
		aggr:       aggr,
		addr:       "b",
		tabletType: topodatapb.TabletType_MASTER,
		elapsed:    9 * time.Millisecond,
		hasError:   false,
	}
	aggr.processQueryInfo(qi)
	t.Logf("aggr.processQueryInfo(b, master, 9ms, false)")
	qi = &queryInfo{
		aggr:       aggr,
		addr:       "",
		tabletType: topodatapb.TabletType_MASTER,
		elapsed:    6 * time.Millisecond,
		hasError:   true,
	}
	aggr.processQueryInfo(qi)
	t.Logf("aggr.processQueryInfo(, master, 6ms, true)")
	want = &TabletCacheStatus{
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

	// Make sure the HTML rendering of the cache works.
	// This will catch most typos.
	templ, err := template.New("").Parse(StatusTemplate)
	if err != nil {
		t.Fatalf("error parsing template: %v", err)
	}
	wr := &bytes.Buffer{}
	if err := templ.Execute(wr, []*TabletCacheStatus{got}); err != nil {
		t.Fatalf("error executing template: %v", err)
	}
}
