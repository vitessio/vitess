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
	vars := framework.DebugVars()
	cases := []struct {
		tag string
		val int
	}{{
		tag: "ConnPoolAvailable",
		val: tabletenv.Config.PoolSize,
	}, {
		tag: "ConnPoolCapacity",
		val: tabletenv.Config.PoolSize,
	}, {
		tag: "ConnPoolIdleTimeout",
		val: int(tabletenv.Config.IdleTimeout * 1e9),
	}, {
		tag: "ConnPoolMaxCap",
		val: tabletenv.Config.PoolSize,
	}, {
		tag: "MaxDMLRows",
		val: tabletenv.Config.MaxDMLRows,
	}, {
		tag: "MaxResultSize",
		val: tabletenv.Config.MaxResultSize,
	}, {
		tag: "WarnResultSize",
		val: tabletenv.Config.WarnResultSize,
	}, {
		tag: "QueryCacheCapacity",
		val: tabletenv.Config.QueryPlanCacheSize,
	}, {
		tag: "QueryTimeout",
		val: int(tabletenv.Config.QueryTimeout * 1e9),
	}, {
		tag: "QueryPoolTimeout",
		val: int(tabletenv.Config.QueryPoolTimeout * 1e9),
	}, {
		tag: "SchemaReloadTime",
		val: int(tabletenv.Config.SchemaReloadTime * 1e9),
	}, {
		tag: "StreamBufferSize",
		val: tabletenv.Config.StreamBufferSize,
	}, {
		tag: "StreamConnPoolAvailable",
		val: tabletenv.Config.StreamPoolSize,
	}, {
		tag: "StreamConnPoolCapacity",
		val: tabletenv.Config.StreamPoolSize,
	}, {
		tag: "StreamConnPoolIdleTimeout",
		val: int(tabletenv.Config.IdleTimeout * 1e9),
	}, {
		tag: "StreamConnPoolMaxCap",
		val: tabletenv.Config.StreamPoolSize,
	}, {
		tag: "TransactionPoolAvailable",
		val: tabletenv.Config.TransactionCap,
	}, {
		tag: "TransactionPoolCapacity",
		val: tabletenv.Config.TransactionCap,
	}, {
		tag: "TransactionPoolIdleTimeout",
		val: int(tabletenv.Config.IdleTimeout * 1e9),
	}, {
		tag: "TransactionPoolMaxCap",
		val: tabletenv.Config.TransactionCap,
	}, {
		tag: "TransactionPoolTimeout",
		val: int(tabletenv.Config.TxPoolTimeout * 1e9),
	}, {
		tag: "TransactionTimeout",
		val: int(tabletenv.Config.TransactionTimeout * 1e9),
	}}
	for _, tcase := range cases {
		if err := verifyIntValue(vars, tcase.tag, tcase.val); err != nil {
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
	framework.Server.SetConsolidatorEnabled(false)
	defer framework.Server.SetConsolidatorEnabled(true)
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

	framework.Server.SetConsolidatorEnabled(false)
	defer framework.Server.SetConsolidatorEnabled(true)
	framework.Server.SetConsolidatorReplicasEnabled(true)
	defer framework.Server.SetConsolidatorReplicasEnabled(false)

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
	if code := vterrors.Code(err); code != vtrpcpb.Code_DEADLINE_EXCEEDED {
		t.Errorf("Error code: %v, want %v", code, vtrpcpb.Code_DEADLINE_EXCEEDED)
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

func TestQueryPoolTimeout(t *testing.T) {
	vstart := framework.DebugVars()

	defer framework.Server.SetQueryPoolTimeout(framework.Server.GetQueryPoolTimeout())
	framework.Server.SetQueryPoolTimeout(100 * time.Millisecond)
	defer framework.Server.SetPoolSize(framework.Server.PoolSize())
	framework.Server.SetPoolSize(1)

	client := framework.NewClient()

	ch := make(chan error)
	go func() {
		_, qerr := framework.NewClient().Execute("select sleep(0.5) from dual", nil)
		ch <- qerr
	}()
	// The queries have to be different so consolidator doesn't kick in.
	go func() {
		_, qerr := framework.NewClient().Execute("select sleep(0.49) from dual", nil)
		ch <- qerr
	}()

	err1 := <-ch
	err2 := <-ch

	if err1 == nil && err2 == nil {
		t.Errorf("both queries unexpectedly succeeded")
	}
	if err1 != nil && err2 != nil {
		t.Errorf("both queries unexpectedly failed")
	}

	var err error
	if err1 != nil {
		err = err1
	} else {
		err = err2
	}

	if code := vterrors.Code(err); code != vtrpcpb.Code_RESOURCE_EXHAUSTED {
		t.Errorf("Error code: %v, want %v", code, vtrpcpb.Code_RESOURCE_EXHAUSTED)
	}

	// Test that this doesn't override the query timeout
	defer framework.Server.QueryTimeout.Set(framework.Server.QueryTimeout.Get())
	framework.Server.QueryTimeout.Set(100 * time.Millisecond)

	_, err = client.Execute("select sleep(1) from vitess_test", nil)
	if code := vterrors.Code(err); code != vtrpcpb.Code_DEADLINE_EXCEEDED {
		t.Errorf("Error code: %v, want %v", code, vtrpcpb.Code_DEADLINE_EXCEEDED)
	}

	vend := framework.DebugVars()
	if err := verifyIntValue(vend, "QueryPoolTimeout", int(100*time.Millisecond)); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "QueryTimeout", int(100*time.Millisecond)); err != nil {
		t.Error(err)
	}
	if err := compareIntDiff(vend, "Kills/Queries", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestConnPoolWaitCap(t *testing.T) {
	vstart := framework.DebugVars()

	defer framework.Server.SetPoolSize(framework.Server.PoolSize())
	framework.Server.SetPoolSize(1)

	defer framework.Server.SetQueryPoolWaiterCap(framework.Server.GetQueryPoolWaiterCap())
	framework.Server.SetQueryPoolWaiterCap(1)

	defer framework.Server.SetTxPoolWaiterCap(framework.Server.GetTxPoolWaiterCap())
	framework.Server.SetTxPoolWaiterCap(1)

	ch := make(chan error)
	go func() {
		_, qerr := framework.NewClient().Execute("select sleep(0.5) from dual", nil)
		ch <- qerr
	}()
	// The queries have to be different so consolidator doesn't kick in.
	go func() {
		_, qerr := framework.NewClient().Execute("select sleep(0.49) from dual", nil)
		ch <- qerr
	}()
	go func() {
		_, qerr := framework.NewClient().Execute("select sleep(0.48) from dual", nil)
		ch <- qerr
	}()

	err1 := <-ch
	err2 := <-ch
	err3 := <-ch

	if err1 == nil && err2 == nil && err3 == nil {
		t.Errorf("all queries unexpectedly succeeded")
	}
	if err1 != nil && err2 != nil && err3 != nil {
		t.Errorf("all queries unexpectedly failed")
	}

	// At least one of the queries should have failed
	var err error
	if err1 != nil {
		err = err1
	}
	if err2 != nil {
		err = err2
	}
	if err3 != nil {
		err = err3
	}

	if code := vterrors.Code(err); code != vtrpcpb.Code_RESOURCE_EXHAUSTED {
		t.Errorf("Error code: %v, want %v", code, vtrpcpb.Code_RESOURCE_EXHAUSTED)
	}

	wantErr := "query pool waiter count exceeded"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("Error: %v, want %v", err, wantErr)
	}

	// Test the same thing with transactions
	go func() {
		client := framework.NewClient()
		beginErr := client.Begin(false)
		if beginErr != nil {
			ch <- beginErr
			return
		}

		_, qerr := client.Execute("update vitess_a set foo='bar' where eid = 123", nil)
		client.Rollback()
		ch <- qerr
	}()
	go func() {
		client := framework.NewClient()
		beginErr := client.Begin(false)
		if beginErr != nil {
			ch <- beginErr
			return
		}

		_, qerr := client.Execute("update vitess_a set foo='bar' where eid = 456", nil)
		client.Rollback()
		ch <- qerr
	}()

	err1 = <-ch
	err2 = <-ch

	if err1 == nil && err2 == nil {
		t.Errorf("both queries unexpectedly succeeded")
	}
	if err1 != nil && err2 != nil {
		t.Errorf("both queries unexpectedly failed")
	}

	if err1 != nil {
		err = err1
	} else {
		err = err2
	}

	if code := vterrors.Code(err); code != vtrpcpb.Code_RESOURCE_EXHAUSTED {
		t.Errorf("Error code: %v, want %v", code, vtrpcpb.Code_RESOURCE_EXHAUSTED)
	}

	wantErr = "transaction pool waiter count exceeded"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("Error: %v, want %v", err, wantErr)
	}

	vend := framework.DebugVars()
	if err := compareIntDiff(vend, "Kills/Queries", vstart, 0); err != nil {
		t.Error(err)
	}
}
