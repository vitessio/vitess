/*
Copyright 2019 The Vitess Authors.

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

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestTabletStatusAggregator(t *testing.T) {
	aggr := &TabletStatusAggregator{
		Keyspace:   "k",
		Shard:      "s",
		TabletType: topodatapb.TabletType_REPLICA,
		Name:       "n",
		Addr:       "a",
	}
	qi := &queryInfo{
		aggr:       aggr,
		addr:       "",
		tabletType: topodatapb.TabletType_REPLICA,
		elapsed:    10 * time.Millisecond,
		hasError:   false,
	}
	aggr.processQueryInfo(qi)
	aggr.resetNextSlot()
	qi = &queryInfo{
		aggr:       aggr,
		addr:       "",
		tabletType: topodatapb.TabletType_REPLICA,
		elapsed:    8 * time.Millisecond,
		hasError:   false,
	}
	aggr.processQueryInfo(qi)
	qi = &queryInfo{
		aggr:       aggr,
		addr:       "",
		tabletType: topodatapb.TabletType_REPLICA,
		elapsed:    3 * time.Millisecond,
		hasError:   true,
	}
	aggr.processQueryInfo(qi)
	want := &TabletCacheStatus{
		Keyspace:   "k",
		Shard:      "s",
		Name:       "n",
		TabletType: topodatapb.TabletType_REPLICA,
		Addr:       "a",
		QueryCount: 3,
		QueryError: 1,
		QPS:        0.05,
		AvgLatency: 7,
	}
	got := aggr.GetCacheStatus()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("aggr.GetCacheStatus() =\n%+v, want =\n%+v", got, want)
	}
	// reset values in idx=0
	for i := 0; i < 59; i++ {
		aggr.resetNextSlot()
	}
	qi = &queryInfo{
		aggr:       aggr,
		addr:       "b",
		tabletType: topodatapb.TabletType_MASTER,
		elapsed:    9 * time.Millisecond,
		hasError:   false,
	}
	aggr.processQueryInfo(qi)
	qi = &queryInfo{
		aggr:       aggr,
		addr:       "",
		tabletType: topodatapb.TabletType_MASTER,
		elapsed:    6 * time.Millisecond,
		hasError:   true,
	}
	aggr.processQueryInfo(qi)
	want = &TabletCacheStatus{
		Keyspace:   "k",
		Shard:      "s",
		Name:       "n",
		TabletType: topodatapb.TabletType_MASTER,
		Addr:       "b",
		QueryCount: 2,
		QueryError: 1,
		QPS:        0.03333333333333333,
		AvgLatency: 7.5,
	}
	got = aggr.GetCacheStatus()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("aggr.GetCacheStatus() =\n%+v, want =\n%+v", got, want)
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
