// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"golang.org/x/net/context"
)

func TestTxEngineClose(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	dbconfigs := testUtils.newDBConfigs(db)
	ctx := context.Background()
	config := tabletenv.DefaultQsConfig
	config.TransactionCap = 10
	config.TransactionTimeout = 0.5
	config.TxShutDownGracePeriod = 0
	te := NewTxEngine(nil, config)

	// Normal close.
	te.Open(dbconfigs)
	start := time.Now()
	te.Close(false)
	if diff := time.Now().Sub(start); diff > 500*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.5s", diff)
	}

	// Normal close with timeout wait.
	te.Open(dbconfigs)
	c, err := te.txPool.LocalBegin(ctx)
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
	te.Open(dbconfigs)
	c, err = te.txPool.LocalBegin(ctx)
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
	te.Open(dbconfigs)
	c, err = te.txPool.LocalBegin(ctx)
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
	te.Open(dbconfigs)
	c, err = te.txPool.LocalBegin(ctx)
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
	te.Open(dbconfigs)
	c, err = te.txPool.LocalBegin(ctx)
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
