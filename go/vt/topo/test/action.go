// package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
)

func CheckActions(t *testing.T, ts topo.Server) {
	cell := getLocalCell(t, ts)
	tablet := &topo.Tablet{
		Cell:     cell,
		Uid:      1,
		Parent:   topo.TabletAlias{},
		Addr:     "localhost:3333",
		Keyspace: "test_keyspace",
		Type:     topo.TYPE_MASTER,
		State:    topo.STATE_READ_WRITE,
		KeyRange: newKeyRange("-10"),
	}
	if err := ts.CreateTablet(tablet); err != nil {
		t.Fatalf("CreateTablet: %v", err)
	}
	tabletAlias := topo.TabletAlias{Cell: cell, Uid: 1}

	actionPath, err := ts.WriteTabletAction(tabletAlias, "contents1")
	if err != nil {
		t.Fatalf("WriteTabletAction: %v", err)
	}

	interrupted := make(chan struct{}, 1)
	if _, err := ts.WaitForTabletAction(actionPath, time.Second/100, interrupted); err != topo.ErrTimeout {
		t.Errorf("WaitForTabletAction returned %v", err)
	}
	go func() {
		time.Sleep(time.Second / 10)
		close(interrupted)
	}()
	if _, err := ts.WaitForTabletAction(actionPath, time.Second*5, interrupted); err != topo.ErrInterrupted {
		t.Errorf("WaitForTabletAction returned %v", err)
	}

	wg := sync.WaitGroup{}

	// wait for the result in one thread
	wg.Add(1)
	go func() {
		interrupted := make(chan struct{}, 1)
		result, err := ts.WaitForTabletAction(actionPath, time.Second*10, interrupted)
		if err != nil {
			t.Errorf("WaitForTabletAction returned %v", err)
		}
		if result != "contents3" {
			t.Errorf("WaitForTabletAction returned bad result: %v", result)
		}
		wg.Done()
	}()

	// process the action in another thread
	done := make(chan struct{}, 1)
	wg.Add(1)
	go ts.ActionEventLoop(tabletAlias, func(ap, data string) error {
		// the actionPath sent back to the action processor
		// is the exact one we have in normal cases,
		// but for the tee, we add extra information.
		if ap != actionPath && !strings.HasSuffix(ap, actionPath) {
			t.Errorf("Bad action path: %v", ap)
		}
		if data != "contents1" {
			t.Errorf("Bad data: %v", data)
		}
		ta, contents, version, err := ts.ReadTabletActionPath(ap)
		if err != nil {
			t.Errorf("Error from ReadTabletActionPath: %v", err)
		}
		if contents != data {
			t.Errorf("Bad contents: %v", contents)
		}
		if ta != tabletAlias {
			t.Errorf("Bad tablet alias: %v", ta)
		}

		if err := ts.UpdateTabletAction(ap, "contents2", version); err != nil {
			t.Errorf("UpdateTabletAction failed: %v", err)
		}
		if err := ts.StoreTabletActionResponse(ap, "contents3"); err != nil {
			t.Errorf("StoreTabletActionResponse failed: %v", err)
		}
		if err := ts.UnblockTabletAction(ap); err != nil {
			t.Errorf("UnblockTabletAction failed: %v", err)
		}

		wg.Done()
		return nil
	}, done)
	close(done)
	wg.Wait()
}
