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

package tabletserver

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"golang.org/x/net/context"
)

func TestTxEngineClose(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	dbcfgs := testUtils.newDBConfigs(db)
	ctx := context.Background()
	config := tabletenv.DefaultQsConfig
	config.TransactionCap = 10
	config.TransactionTimeout = 0.5
	config.TxShutDownGracePeriod = 0
	te := NewTxEngine(nil, config)
	te.InitDBConfig(dbcfgs)

	// Normal close.
	te.Open()
	start := time.Now()
	te.Close(false)
	if diff := time.Now().Sub(start); diff > 500*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.5s", diff)
	}

	// Normal close with timeout wait.
	te.Open()
	c, err := te.txPool.LocalBegin(ctx, false, querypb.ExecuteOptions_DEFAULT)
	if err != nil {
		t.Fatal(err)
	}
	c.Recycle()
	start = time.Now()
	te.Close(false)
	if diff := time.Now().Sub(start); diff < 500*time.Millisecond {
		t.Errorf("Close time: %v, must be over 0.5s", diff)
	}

	// Immediate close.
	te.Open()
	c, err = te.txPool.LocalBegin(ctx, false, querypb.ExecuteOptions_DEFAULT)
	if err != nil {
		t.Fatal(err)
	}
	c.Recycle()
	start = time.Now()
	te.Close(true)
	if diff := time.Now().Sub(start); diff > 500*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.5s", diff)
	}

	// Normal close with short grace period.
	te.shutdownGracePeriod = 250 * time.Millisecond
	te.Open()
	c, err = te.txPool.LocalBegin(ctx, false, querypb.ExecuteOptions_DEFAULT)
	if err != nil {
		t.Fatal(err)
	}
	c.Recycle()
	start = time.Now()
	te.Close(false)
	if diff := time.Now().Sub(start); diff > 500*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.5s", diff)
	}
	if diff := time.Now().Sub(start); diff < 250*time.Millisecond {
		t.Errorf("Close time: %v, must be over 0.25s", diff)
	}

	// Normal close with short grace period, but pool gets empty early.
	te.shutdownGracePeriod = 250 * time.Millisecond
	te.Open()
	c, err = te.txPool.LocalBegin(ctx, false, querypb.ExecuteOptions_DEFAULT)
	if err != nil {
		t.Fatal(err)
	}
	c.Recycle()
	go func() {
		time.Sleep(100 * time.Millisecond)
		_, err := te.txPool.Get(c.TransactionID, "return")
		if err != nil {
			t.Error(err)
		}
		te.txPool.LocalConclude(ctx, c)
	}()
	start = time.Now()
	te.Close(false)
	if diff := time.Now().Sub(start); diff > 250*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.25s", diff)
	}
	if diff := time.Now().Sub(start); diff < 100*time.Millisecond {
		t.Errorf("Close time: %v, must be over 0.1", diff)
	}

	// Immediate close, but connection is in use.
	te.Open()
	c, err = te.txPool.LocalBegin(ctx, false, querypb.ExecuteOptions_DEFAULT)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		te.txPool.LocalConclude(ctx, c)
	}()
	start = time.Now()
	te.Close(true)
	if diff := time.Now().Sub(start); diff > 250*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.25s", diff)
	}
	if diff := time.Now().Sub(start); diff < 100*time.Millisecond {
		t.Errorf("Close time: %v, must be over 0.1", diff)
	}
}
