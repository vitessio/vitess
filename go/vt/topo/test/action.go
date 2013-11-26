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
		Alias:    topo.TabletAlias{Cell: cell, Uid: 1},
		Hostname: "localhost",
		IPAddr:   "10.11.12.13",
		Portmap: map[string]int{
			"vt":    3333,
			"mysql": 3334,
		},
		Parent:   topo.TabletAlias{},
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

	// wait for the result in one thread
	wg1 := sync.WaitGroup{}
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		interrupted := make(chan struct{}, 1)
		var err error
		for i := 0; i <= 30; i++ {
			var result string
			result, err = ts.WaitForTabletAction(actionPath, time.Second, interrupted)
			if err == topo.ErrTimeout {
				// we waited for one second, didn't see the
				// result, try again up to 30 seconds.
				t.Logf("WaitForTabletAction timed out at try %v/30", i)
				continue
			}
			if err != nil {
				t.Errorf("WaitForTabletAction returned: %v", err)
			}
			if result != "contents3" {
				t.Errorf("WaitForTabletAction returned bad result: %v", result)
			}
			return
		}
		t.Errorf("WaitForTabletAction timed out: %v", err)
	}()

	// process the action in another thread
	done := make(chan struct{}, 1)
	wg2 := sync.WaitGroup{}
	wg2.Add(1)
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

		wg2.Done()
		return nil
	}, done)

	// first wait for the processing to be done, then close the
	// action loop, then wait for the response to be received.
	wg2.Wait()
	close(done)
	wg1.Wait()
}
