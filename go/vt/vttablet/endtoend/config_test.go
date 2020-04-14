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

package endtoend

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// compareIntDiff returns an error if end[tag] != start[tag]+diff.
func compareIntDiff(end map[string]interface{}, tag string, start map[string]interface{}, diff int) error {
	return verifyIntValue(end, tag, framework.FetchInt(start, tag)+diff)
}

// verifyIntValue returns an error if values[tag] != want.
func verifyIntValue(values map[string]interface{}, tag string, want int) error {
	got := framework.FetchInt(values, tag)
	if got != want {
		return fmt.Errorf("%s: %d, want %d", tag, got, want)
	}
	return nil
}

func TestConfigVars(t *testing.T) {
	currentConfig := tabletenv.NewCurrentConfig()
	vars := framework.DebugVars()
	cases := []struct {
		tag string
		val int
	}{{
		tag: "ConnPoolAvailable",
		val: currentConfig.OltpReadPool.Size,
	}, {
		tag: "ConnPoolCapacity",
		val: currentConfig.OltpReadPool.Size,
	}, {
		tag: "ConnPoolIdleTimeout",
		val: currentConfig.OltpReadPool.IdleTimeoutSeconds * 1e9,
	}, {
		tag: "ConnPoolMaxCap",
		val: currentConfig.OltpReadPool.Size,
	}, {
		tag: "MaxResultSize",
		val: currentConfig.Oltp.MaxRows,
	}, {
		tag: "WarnResultSize",
		val: currentConfig.Oltp.WarnRows,
	}, {
		tag: "QueryCacheCapacity",
		val: currentConfig.QueryPlanCacheSize,
	}, {
		tag: "QueryTimeout",
		val: int(currentConfig.Oltp.QueryTimeoutSeconds * 1e9),
	}, {
		tag: "SchemaReloadTime",
		val: int(currentConfig.SchemaReloadTime * 1e9),
	}, {
		tag: "StreamBufferSize",
		val: currentConfig.StreamBufferSize,
	}, {
		tag: "StreamConnPoolAvailable",
		val: currentConfig.OlapReadPool.Size,
	}, {
		tag: "StreamConnPoolCapacity",
		val: currentConfig.OlapReadPool.Size,
	}, {
		tag: "StreamConnPoolIdleTimeout",
		val: currentConfig.OlapReadPool.IdleTimeoutSeconds * 1e9,
	}, {
		tag: "StreamConnPoolMaxCap",
		val: currentConfig.OlapReadPool.Size,
	}, {
		tag: "TransactionPoolAvailable",
		val: currentConfig.TxPool.Size,
	}, {
		tag: "TransactionPoolCapacity",
		val: currentConfig.TxPool.Size,
	}, {
		tag: "TransactionPoolIdleTimeout",
		val: currentConfig.TxPool.IdleTimeoutSeconds * 1e9,
	}, {
		tag: "TransactionPoolMaxCap",
		val: currentConfig.TxPool.Size,
	}, {
		tag: "TransactionTimeout",
		val: int(currentConfig.Oltp.TxTimeoutSeconds * 1e9),
	}}
	for _, tcase := range cases {
		if err := verifyIntValue(vars, tcase.tag, int(tcase.val)); err != nil {
			t.Error(err)
		}
	}
}

func TestPoolSize(t *testing.T) {
	defer framework.Server.SetPoolSize(framework.Server.PoolSize())
	framework.Server.SetPoolSize(1)

	vstart := framework.DebugVars()
	if err := verifyIntValue(vstart, "ConnPoolCapacity", 1); err != nil {
		t.Error(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		framework.NewClient().Execute("select sleep(0.5) from dual", nil)
		wg.Done()
	}()
	// The queries have to be different so consolidator doesn't kick in.
	go func() {
		framework.NewClient().Execute("select sleep(0.49) from dual", nil)
		wg.Done()
	}()
	wg.Wait()

	// Parallel plan building can cause multiple conn pool waits.
	// Check that the wait count was at least incremented once so
	// we know it's working.
	tag := "ConnPoolWaitCount"
	got := framework.FetchInt(framework.DebugVars(), tag)
	want := framework.FetchInt(vstart, tag)
	if got <= want {
		t.Errorf("%s: %d, must be greater than %d", tag, got, want)
	}
}

func TestDisableConsolidator(t *testing.T) {
	totalConsolidationsTag := "Waits/Histograms/Consolidations/inf"
	initial := framework.FetchInt(framework.DebugVars(), totalConsolidationsTag)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		framework.NewClient().Execute("select sleep(0.5) from dual", nil)
		wg.Done()
	}()
	go func() {
		framework.NewClient().Execute("select sleep(0.5) from dual", nil)
		wg.Done()
	}()
	wg.Wait()
	afterOne := framework.FetchInt(framework.DebugVars(), totalConsolidationsTag)
	if initial+1 != afterOne {
		t.Errorf("expected one consolidation, but got: before consolidation count: %v; after consolidation count: %v", initial, afterOne)
	}
	framework.Server.SetConsolidatorMode(tabletenv.Disable)
	defer framework.Server.SetConsolidatorMode(tabletenv.Enable)
	var wg2 sync.WaitGroup
	wg2.Add(2)
	go func() {
		framework.NewClient().Execute("select sleep(0.5) from dual", nil)
		wg2.Done()
	}()
	go func() {
		framework.NewClient().Execute("select sleep(0.5) from dual", nil)
		wg2.Done()
	}()
	wg2.Wait()
	noNewConsolidations := framework.FetchInt(framework.DebugVars(), totalConsolidationsTag)
	if afterOne != noNewConsolidations {
		t.Errorf("expected no new consolidations, but got: before consolidation count: %v; after consolidation count: %v", afterOne, noNewConsolidations)
	}
}

func TestConsolidatorReplicasOnly(t *testing.T) {
	totalConsolidationsTag := "Waits/Histograms/Consolidations/inf"
	initial := framework.FetchInt(framework.DebugVars(), totalConsolidationsTag)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		framework.NewClient().Execute("select sleep(0.5) from dual", nil)
		wg.Done()
	}()
	go func() {
		framework.NewClient().Execute("select sleep(0.5) from dual", nil)
		wg.Done()
	}()
	wg.Wait()
	afterOne := framework.FetchInt(framework.DebugVars(), totalConsolidationsTag)
	if initial+1 != afterOne {
		t.Errorf("expected one consolidation, but got: before consolidation count: %v; after consolidation count: %v", initial, afterOne)
	}

	framework.Server.SetConsolidatorMode(tabletenv.NotOnMaster)
	defer framework.Server.SetConsolidatorMode(tabletenv.Enable)

	// master should not do query consolidation
	var wg2 sync.WaitGroup
	wg2.Add(2)
	go func() {
		framework.NewClient().Execute("select sleep(0.5) from dual", nil)
		wg2.Done()
	}()
	go func() {
		framework.NewClient().Execute("select sleep(0.5) from dual", nil)
		wg2.Done()
	}()
	wg2.Wait()
	noNewConsolidations := framework.FetchInt(framework.DebugVars(), totalConsolidationsTag)
	if afterOne != noNewConsolidations {
		t.Errorf("expected no new consolidations, but got: before consolidation count: %v; after consolidation count: %v", afterOne, noNewConsolidations)
	}

	// become a replica, where query consolidation should happen
	client := framework.NewClientWithTabletType(topodatapb.TabletType_REPLICA)

	err := client.SetServingType(topodatapb.TabletType_REPLICA)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = client.SetServingType(topodatapb.TabletType_MASTER)
		if err != nil {
			t.Fatal(err)
		}
	}()

	initial = framework.FetchInt(framework.DebugVars(), totalConsolidationsTag)
	var wg3 sync.WaitGroup
	wg3.Add(2)
	go func() {
		client.Execute("select sleep(0.5) from dual", nil)
		wg3.Done()
	}()
	go func() {
		client.Execute("select sleep(0.5) from dual", nil)
		wg3.Done()
	}()
	wg3.Wait()
	afterOne = framework.FetchInt(framework.DebugVars(), totalConsolidationsTag)
	if initial+1 != afterOne {
		t.Errorf("expected another consolidation, but got: before consolidation count: %v; after consolidation count: %v", initial, afterOne)
	}
}

func TestQueryPlanCache(t *testing.T) {
	defer framework.Server.SetQueryPlanCacheCap(framework.Server.QueryPlanCacheCap())
	framework.Server.SetQueryPlanCacheCap(1)

	bindVars := map[string]*querypb.BindVariable{
		"ival1": sqltypes.Int64BindVariable(1),
		"ival2": sqltypes.Int64BindVariable(1),
	}
	client := framework.NewClient()
	_, _ = client.Execute("select * from vitess_test where intval=:ival1", bindVars)
	_, _ = client.Execute("select * from vitess_test where intval=:ival2", bindVars)
	vend := framework.DebugVars()
	if err := verifyIntValue(vend, "QueryCacheLength", 1); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "QueryCacheSize", 1); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "QueryCacheCapacity", 1); err != nil {
		t.Error(err)
	}

	framework.Server.SetQueryPlanCacheCap(10)
	_, _ = client.Execute("select * from vitess_test where intval=:ival1", bindVars)
	vend = framework.DebugVars()
	if err := verifyIntValue(vend, "QueryCacheLength", 2); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "QueryCacheSize", 2); err != nil {
		t.Error(err)
	}

	_, _ = client.Execute("select * from vitess_test where intval=1", bindVars)
	vend = framework.DebugVars()
	if err := verifyIntValue(vend, "QueryCacheLength", 3); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "QueryCacheSize", 3); err != nil {
		t.Error(err)
	}
}

func TestMaxResultSize(t *testing.T) {
	defer framework.Server.SetMaxResultSize(framework.Server.MaxResultSize())
	framework.Server.SetMaxResultSize(2)

	client := framework.NewClient()
	query := "select * from vitess_test"
	_, err := client.Execute(query, nil)
	want := "Row count exceeded"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, must start with %s", err, want)
	}
	if err := verifyIntValue(framework.DebugVars(), "MaxResultSize", 2); err != nil {
		t.Error(err)
	}

	framework.Server.SetMaxResultSize(10)
	_, err = client.Execute(query, nil)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestWarnResultSize(t *testing.T) {
	defer framework.Server.SetWarnResultSize(framework.Server.WarnResultSize())
	framework.Server.SetWarnResultSize(2)
	client := framework.NewClient()

	originalWarningsResultsExceededCount := framework.FetchInt(framework.DebugVars(), "Warnings/ResultsExceeded")
	query := "select * from vitess_test"
	_, _ = client.Execute(query, nil)
	newWarningsResultsExceededCount := framework.FetchInt(framework.DebugVars(), "Warnings/ResultsExceeded")
	exceededCountDiff := newWarningsResultsExceededCount - originalWarningsResultsExceededCount
	if exceededCountDiff != 1 {
		t.Errorf("Warnings.ResultsExceeded counter should have increased by 1, instead got %v", exceededCountDiff)
	}

	if err := verifyIntValue(framework.DebugVars(), "WarnResultSize", 2); err != nil {
		t.Error(err)
	}

	framework.Server.SetWarnResultSize(10)
	_, _ = client.Execute(query, nil)
	newerWarningsResultsExceededCount := framework.FetchInt(framework.DebugVars(), "Warnings/ResultsExceeded")
	exceededCountDiff = newerWarningsResultsExceededCount - newWarningsResultsExceededCount
	if exceededCountDiff != 0 {
		t.Errorf("Warnings.ResultsExceeded counter should not have increased, instead got %v", exceededCountDiff)
	}
}

func TestQueryTimeout(t *testing.T) {
	vstart := framework.DebugVars()
	defer framework.Server.QueryTimeout.Set(framework.Server.QueryTimeout.Get())
	framework.Server.QueryTimeout.Set(100 * time.Millisecond)

	client := framework.NewClient()
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute("select sleep(1) from vitess_test", nil)
	if code := vterrors.Code(err); code != vtrpcpb.Code_CANCELED {
		t.Errorf("Error code: %v, want %v", code, vtrpcpb.Code_CANCELED)
	}
	_, err = client.Execute("select 1 from dual", nil)
	if code := vterrors.Code(err); code != vtrpcpb.Code_ABORTED {
		t.Errorf("Error code: %v, want %v", code, vtrpcpb.Code_ABORTED)
	}
	vend := framework.DebugVars()
	if err := verifyIntValue(vend, "QueryTimeout", int(100*time.Millisecond)); err != nil {
		t.Error(err)
	}
	if err := compareIntDiff(vend, "Kills/Queries", vstart, 1); err != nil {
		t.Error(err)
	}
}
