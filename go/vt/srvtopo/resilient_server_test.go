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

package srvtopo

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"reflect"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/key"

	"vitess.io/vitess/go/sync2"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/status"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// TestGetSrvKeyspace will test we properly return updated SrvKeyspace.
func TestGetSrvKeyspace(t *testing.T) {
	ts, factory := memorytopo.NewServerAndFactory("test_cell")
	srvTopoCacheTTL = 200 * time.Millisecond
	srvTopoCacheRefresh = 80 * time.Millisecond
	defer func() {
		srvTopoCacheTTL = 1 * time.Second
		srvTopoCacheRefresh = 1 * time.Second
	}()

	rs := NewResilientServer(ts, "TestGetSrvKeyspace")

	// Ask for a not-yet-created keyspace
	_, err := rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
	if !topo.IsErrType(err, topo.NoNode) {
		t.Fatalf("GetSrvKeyspace(not created) got unexpected error: %v", err)
	}

	// Wait until the cached error expires.
	time.Sleep(srvTopoCacheRefresh + 10*time.Millisecond)

	// Set SrvKeyspace with value
	want := &topodatapb.SrvKeyspace{}
	err = ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)
	require.NoError(t, err, "UpdateSrvKeyspace(test_cell, test_ks, %s) failed", want)

	// wait until we get the right value
	var got *topodatapb.SrvKeyspace
	expiry := time.Now().Add(srvTopoCacheRefresh - 20*time.Millisecond)
	for {
		ctx, cancel := context.WithCancel(context.Background())
		got, err = rs.GetSrvKeyspace(ctx, "test_cell", "test_ks")
		cancel()

		if err != nil {
			t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
		}
		if proto.Equal(want, got) {
			break
		}
		if time.Now().After(expiry) {
			t.Fatalf("GetSrvKeyspace() timeout = %+v, want %+v", got, want)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Update the value and check it again to verify that the watcher
	// is still up and running
	want = &topodatapb.SrvKeyspace{Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{{ServedType: topodatapb.TabletType_REPLICA}}}
	err = ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)
	require.NoError(t, err, "UpdateSrvKeyspace(test_cell, test_ks, %s) failed", want)

	// Wait a bit to give the watcher enough time to update the value.
	time.Sleep(10 * time.Millisecond)
	got, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")

	if err != nil {
		t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
	}
	if !proto.Equal(want, got) {
		t.Fatalf("GetSrvKeyspace() = %+v, want %+v", got, want)
	}

	// make sure the HTML template works
	funcs := map[string]any{}
	for k, v := range status.StatusFuncs {
		funcs[k] = v
	}
	for k, v := range StatusFuncs {
		funcs[k] = v
	}
	templ := template.New("").Funcs(funcs)
	templ, err = templ.Parse(TopoTemplate)
	if err != nil {
		t.Fatalf("error parsing template: %v", err)
	}
	wr := &bytes.Buffer{}
	if err := templ.Execute(wr, rs.CacheStatus()); err != nil {
		t.Fatalf("error executing template: %v", err)
	}

	// Now delete the SrvKeyspace, wait until we get the error.
	if err := ts.DeleteSrvKeyspace(context.Background(), "test_cell", "test_ks"); err != nil {
		t.Fatalf("DeleteSrvKeyspace() failed: %v", err)
	}
	expiry = time.Now().Add(5 * time.Second)
	for {
		_, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		if topo.IsErrType(err, topo.NoNode) {
			break
		}
		if time.Now().After(expiry) {
			t.Fatalf("timeout waiting for no keyspace error")
		}
		time.Sleep(time.Millisecond)
	}

	// Now send an updated real value, see it come through.
	keyRange, err := key.ParseShardingSpec("-")
	if err != nil || len(keyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(keyRange))
	}

	want = &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-",
						KeyRange: keyRange[0],
					},
				},
			},
		},
	}

	err = ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)
	require.NoError(t, err, "UpdateSrvKeyspace(test_cell, test_ks, %s) failed", want)
	expiry = time.Now().Add(5 * time.Second)
	updateTime := time.Now()
	for {
		got, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		if err == nil && proto.Equal(want, got) {
			break
		}
		if time.Now().After(expiry) {
			t.Fatalf("timeout waiting for new keyspace value")
		}
		time.Sleep(time.Millisecond)
	}

	// Now simulate a topo service error and see that the last value is
	// cached for at least half of the expected ttl.
	errorTestStart := time.Now()
	errorReqsBefore := rs.counts.Counts()[errorCategory]
	forceErr := topo.NewError(topo.Timeout, "test topo error")
	factory.SetError(forceErr)

	expiry = time.Now().Add(srvTopoCacheTTL / 2)
	for {
		got, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		if err != nil || !proto.Equal(want, got) {
			// On a slow test machine it is possible that we never end up
			// verifying the value is cached because it could take too long to
			// even get into this loop... so log this as an informative message
			// but don't fail the test
			if time.Now().After(expiry) {
				t.Logf("test execution was too slow -- caching was not verified")
				break
			}

			t.Errorf("expected keyspace to be cached for at least %s seconds, got error %v", time.Since(updateTime), err)
		}

		if time.Now().After(expiry) {
			break
		}

		time.Sleep(time.Millisecond)
	}

	// Now wait for the TTL to expire and we should get the expected error
	expiry = time.Now().Add(1 * time.Second)
	for {
		_, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		if err != nil || err == forceErr {
			break
		}

		if time.Now().After(expiry) {
			t.Fatalf("timed out waiting for error to be returned")
		}
		time.Sleep(time.Millisecond)
	}

	// Clear the error away and check that the cached error is still returned
	// until the refresh interval elapses
	factory.SetError(nil)
	_, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
	if err == nil || err != forceErr {
		t.Errorf("expected error to be cached")
	}

	// Now sleep for the rest of the interval and we should get the value again
	time.Sleep(srvTopoCacheRefresh)
	got, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
	if err != nil || !proto.Equal(want, got) {
		t.Errorf("expected value to be restored, got %v", err)
	}

	// Now sleep for the full TTL before setting the error again to test
	// that even when there is no activity on the key, it is still cached
	// for the full configured TTL.
	time.Sleep(srvTopoCacheTTL)
	forceErr = topo.NewError(topo.Interrupted, "another test topo error")
	factory.SetError(forceErr)

	expiry = time.Now().Add(srvTopoCacheTTL / 2)
	for {
		_, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		if err != nil {
			t.Fatalf("value should have been cached for the full ttl, error %v", err)
		}
		if time.Now().After(expiry) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// Wait again until the TTL expires and we get the error
	expiry = time.Now().Add(time.Second)
	for {
		_, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		if err != nil {
			if err == forceErr {
				break
			}
			t.Fatalf("expected %v got %v", forceErr, err)
		}

		if time.Now().After(expiry) {
			t.Fatalf("timed out waiting for error")
		}
		time.Sleep(time.Millisecond)
	}

	factory.SetError(nil)

	// Check that the expected number of errors were counted during the
	// interval
	errorReqs := rs.counts.Counts()[errorCategory]
	expectedErrors := int64(time.Since(errorTestStart) / srvTopoCacheRefresh)
	if errorReqs-errorReqsBefore > expectedErrors {
		t.Errorf("expected <= %v error requests got %d", expectedErrors, errorReqs-errorReqsBefore)
	}

	// Check that the watch now works to update the value
	want = &topodatapb.SrvKeyspace{}
	err = ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)
	require.NoError(t, err, "UpdateSrvKeyspace(test_cell, test_ks, %s) failed", want)
	expiry = time.Now().Add(5 * time.Second)
	for {
		got, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		if err == nil && proto.Equal(want, got) {
			break
		}
		if time.Now().After(expiry) {
			t.Fatalf("timeout waiting for new keyspace value")
		}
		time.Sleep(time.Millisecond)
	}

	// Now test with a new error in which the topo service is locked during
	// the test which prevents all queries from proceeding.
	forceErr = fmt.Errorf("test topo error with factory locked")
	factory.SetError(forceErr)
	factory.Lock()
	go func() {
		time.Sleep(srvTopoCacheRefresh * 2)
		factory.Unlock()
	}()

	expiry = time.Now().Add(srvTopoCacheTTL / 2)
	for {
		got, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		if err != nil || !proto.Equal(want, got) {
			// On a slow test machine it is possible that we never end up
			// verifying the value is cached because it could take too long to
			// even get into this loop... so log this as an informative message
			// but don't fail the test
			if time.Now().After(expiry) {
				t.Logf("test execution was too slow -- caching was not verified")
				break
			}

			t.Errorf("expected keyspace to be cached for at least %s seconds, got error %v", time.Since(updateTime), err)
		}

		if time.Now().After(expiry) {
			break
		}

		time.Sleep(time.Millisecond)
	}

	// Clear the error, wait for things to proceed again
	factory.SetError(nil)
	time.Sleep(srvTopoCacheTTL)

	got, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
	if err != nil || !proto.Equal(want, got) {
		t.Errorf("expected error to clear, got %v", err)
	}

	// Force another error and lock the topo. Then wait for the TTL to
	// expire and verify that the context timeout unblocks the request.

	// TODO(deepthi): Commenting out this test until we fix https://github.com/vitessio/vitess/issues/6134

	/*
		forceErr = fmt.Errorf("force long test error")
		factory.SetError(forceErr)
		factory.Lock()

		time.Sleep(*srvTopoCacheTTL)

		timeoutCtx, cancel := context.WithTimeout(context.Background(), *srvTopoCacheRefresh*2) //nolint
		defer cancel()
		_, err = rs.GetSrvKeyspace(timeoutCtx, "test_cell", "test_ks")
		wantErr := "timed out waiting for keyspace"
		if err == nil || err.Error() != wantErr {
			t.Errorf("expected error '%v', got '%v'", wantErr, err)
		}
		factory.Unlock()
	*/
}

// TestSrvKeyspaceCachedError will test we properly re-try to query
// the topo server upon failure.
func TestSrvKeyspaceCachedError(t *testing.T) {
	ts := memorytopo.NewServer("test_cell")
	srvTopoCacheTTL = 100 * time.Millisecond
	srvTopoCacheRefresh = 40 * time.Millisecond
	defer func() {
		srvTopoCacheTTL = 1 * time.Second
		srvTopoCacheRefresh = 1 * time.Second
	}()
	rs := NewResilientServer(ts, "TestSrvKeyspaceCachedErrors")

	// Ask for an unknown keyspace, should get an error.
	ctx := context.Background()
	_, err := rs.GetSrvKeyspace(ctx, "test_cell", "unknown_ks")
	if err == nil {
		t.Fatalf("First GetSrvKeyspace didn't return an error")
	}
	entry := rs.SrvKeyspaceWatcher.rw.getEntry(&srvKeyspaceKey{"test_cell", "unknown_ks"})
	if err != entry.lastError {
		t.Errorf("Error wasn't saved properly")
	}

	time.Sleep(srvTopoCacheTTL + 10*time.Millisecond)
	// Ask again with a different context, should get an error and
	// save that context.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, err2 := rs.GetSrvKeyspace(ctx, "test_cell", "unknown_ks")
	if err2 == nil {
		t.Fatalf("Second GetSrvKeyspace didn't return an error")
	}
	if err2 != entry.lastError {
		t.Errorf("Error wasn't saved properly")
	}
}

// TestGetSrvKeyspaceCreated will test we properly get the initial
// value if the SrvKeyspace already exists.
func TestGetSrvKeyspaceCreated(t *testing.T) {
	ts := memorytopo.NewServer("test_cell")
	rs := NewResilientServer(ts, "TestGetSrvKeyspaceCreated")

	// Set SrvKeyspace with value.
	want := &topodatapb.SrvKeyspace{}
	err := ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)
	require.NoError(t, err, "UpdateSrvKeyspace(test_cell, test_ks, %s) failed", want)

	// Wait until we get the right value.
	expiry := time.Now().Add(5 * time.Second)
	for {
		got, err := rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		switch {
		case topo.IsErrType(err, topo.NoNode):
			// keep trying
		case err == nil:
			// we got a value, see if it's good
			if proto.Equal(want, got) {
				return
			}
		default:
			t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
		}
		if time.Now().After(expiry) {
			t.Fatalf("GetSrvKeyspace() timeout = %+v, want %+v", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWatchSrvVSchema(t *testing.T) {
	srvTopoCacheRefresh = 10 * time.Millisecond
	ctx := context.Background()
	ts := memorytopo.NewServer("test_cell")
	rs := NewResilientServer(ts, "TestWatchSrvVSchema")

	// mu protects watchValue and watchErr.
	mu := sync.Mutex{}
	var watchValue *vschemapb.SrvVSchema
	var watchErr error
	rs.WatchSrvVSchema(ctx, "test_cell", func(v *vschemapb.SrvVSchema, e error) bool {
		mu.Lock()
		defer mu.Unlock()
		watchValue = v
		watchErr = e
		return true
	})
	get := func() (*vschemapb.SrvVSchema, error) {
		mu.Lock()
		defer mu.Unlock()
		return watchValue, watchErr
	}

	// WatchSrvVSchema won't return until it gets the initial value,
	// which is not there, so we should get watchErr=topo.ErrNoNode.
	if _, err := get(); !topo.IsErrType(err, topo.NoNode) {
		t.Fatalf("WatchSrvVSchema didn't return topo.ErrNoNode at first, but got: %v", err)
	}

	// Save a value, wait for it.
	newValue := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks1": {},
		},
	}
	if err := ts.UpdateSrvVSchema(ctx, "test_cell", newValue); err != nil {
		t.Fatalf("UpdateSrvVSchema failed: %v", err)
	}
	start := time.Now()
	for {
		if v, err := get(); err == nil && proto.Equal(newValue, v) {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("timed out waiting for new SrvVschema")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Update value, wait for it.
	updatedValue := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks2": {},
		},
	}
	if err := ts.UpdateSrvVSchema(ctx, "test_cell", updatedValue); err != nil {
		t.Fatalf("UpdateSrvVSchema failed: %v", err)
	}
	start = time.Now()
	for {
		if v, err := get(); err == nil && proto.Equal(updatedValue, v) {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("timed out waiting for updated SrvVschema")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Delete the value, wait for topo.ErrNoNode
	if err := ts.DeleteSrvVSchema(ctx, "test_cell"); err != nil {
		t.Fatalf("DeleteSrvVSchema failed: %v", err)
	}
	start = time.Now()
	for {
		if _, err := get(); topo.IsErrType(err, topo.NoNode) {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("timed out waiting for deleted SrvVschema")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestGetSrvKeyspaceNames(t *testing.T) {
	ts, factory := memorytopo.NewServerAndFactory("test_cell")
	srvTopoCacheTTL = 100 * time.Millisecond
	srvTopoCacheRefresh = 40 * time.Millisecond
	defer func() {
		srvTopoCacheTTL = 1 * time.Second
		srvTopoCacheRefresh = 1 * time.Second
	}()
	rs := NewResilientServer(ts, "TestGetSrvKeyspaceNames")

	// Set SrvKeyspace with value
	want := &topodatapb.SrvKeyspace{}
	err := ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)
	require.NoError(t, err, "UpdateSrvKeyspace(test_cell, test_ks, %s) failed", want)

	err = ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks2", want)
	require.NoError(t, err, "UpdateSrvKeyspace(test_cell, test_ks2, %s) failed", want)

	ctx := context.Background()
	names, err := rs.GetSrvKeyspaceNames(ctx, "test_cell", false)
	if err != nil {
		t.Errorf("GetSrvKeyspaceNames unexpected error %v", err)
	}
	wantNames := []string{"test_ks", "test_ks2"}

	if !reflect.DeepEqual(names, wantNames) {
		t.Errorf("GetSrvKeyspaceNames got %v want %v", names, wantNames)
	}

	forceErr := fmt.Errorf("force test error")
	factory.SetError(forceErr)

	// Lock the topo for half the duration of the cache TTL to ensure our
	// requests aren't blocked
	factory.Lock()
	go func() {
		time.Sleep(srvTopoCacheTTL / 2)
		factory.Unlock()
	}()

	// Check that we get the cached value until at least the refresh interval
	// elapses but before the TTL expires
	start := time.Now()
	for {
		names, err = rs.GetSrvKeyspaceNames(ctx, "test_cell", false)
		if err != nil {
			t.Errorf("GetSrvKeyspaceNames unexpected error %v", err)
		}

		if !reflect.DeepEqual(names, wantNames) {
			t.Errorf("GetSrvKeyspaceNames got %v want %v", names, wantNames)
		}

		if time.Since(start) >= srvTopoCacheRefresh+10*time.Millisecond {
			break
		}

		time.Sleep(time.Millisecond)
	}

	// Now wait for it to expire from cache
	for {
		_, err = rs.GetSrvKeyspaceNames(ctx, "test_cell", false)
		if err != nil {
			break
		}

		time.Sleep(2 * time.Millisecond)

		if time.Since(start) > 2*time.Second {
			t.Fatalf("expected error after TTL expires")
		}
	}

	if err != forceErr {
		t.Errorf("got error %v want %v", err, forceErr)
	}

	// Now, since the TTL has expired, check that when we ask for stale
	// info, we'll get it.
	_, err = rs.GetSrvKeyspaceNames(ctx, "test_cell", true)
	if err != nil {
		t.Fatalf("expected no error if asking for stale cache data")
	}

	// Now, wait long enough that with a stale ask, we'll get an error
	time.Sleep(srvTopoCacheRefresh*2 + 2*time.Millisecond)
	_, err = rs.GetSrvKeyspaceNames(ctx, "test_cell", true)
	if err != forceErr {
		t.Fatalf("expected an error if asking for really stale cache data")
	}

	// Check that we only checked the topo service 1 or 2 times during the
	// period where we got the cached error.
	cachedReqs, ok := rs.counts.Counts()[cachedCategory]
	if !ok || cachedReqs > 2 {
		t.Errorf("expected <= 2 cached requests got %v", cachedReqs)
	}

	// Clear the error and wait until the cached error state expires
	factory.SetError(nil)

	start = time.Now()
	for {
		names, err = rs.GetSrvKeyspaceNames(ctx, "test_cell", false)
		if err == nil {
			break
		}

		time.Sleep(2 * time.Millisecond)

		if time.Since(start) > 2*time.Second {
			t.Fatalf("expected error after TTL expires")
		}
	}

	if !reflect.DeepEqual(names, wantNames) {
		t.Errorf("GetSrvKeyspaceNames got %v want %v", names, wantNames)
	}

	errorReqs, ok := rs.counts.Counts()[errorCategory]
	if !ok || errorReqs == 0 {
		t.Errorf("expected non-zero error requests got %v", errorReqs)
	}

	// Force another error and lock the topo. Then wait for the TTL to
	// expire and verify that the context timeout unblocks the request.
	forceErr = fmt.Errorf("force long test error")
	factory.SetError(forceErr)
	factory.Lock()

	time.Sleep(srvTopoCacheTTL)

	timeoutCtx, cancel := context.WithTimeout(context.Background(), srvTopoCacheRefresh*2) //nolint
	defer cancel()
	_, err = rs.GetSrvKeyspaceNames(timeoutCtx, "test_cell", false)
	if err != context.DeadlineExceeded {
		t.Errorf("expected error '%v', got '%v'", context.DeadlineExceeded, err.Error())
	}
	factory.Unlock()
}

type watched struct {
	keyspace *topodatapb.SrvKeyspace
	err      error
}

func (w *watched) equals(other *watched) bool {
	if w.keyspace != nil {
		return other.keyspace != nil && proto.Equal(w.keyspace, other.keyspace)
	}
	return w.err == other.err
}

func TestSrvKeyspaceWatcher(t *testing.T) {
	ts, factory := memorytopo.NewServerAndFactory("test_cell")
	srvTopoCacheTTL = 100 * time.Millisecond
	srvTopoCacheRefresh = 40 * time.Millisecond
	defer func() {
		srvTopoCacheTTL = 1 * time.Second
		srvTopoCacheRefresh = 1 * time.Second
	}()

	rs := NewResilientServer(ts, "TestGetSrvKeyspaceWatcher")

	var wmu sync.Mutex
	var wseen []watched

	allSeen := func() []watched {
		wmu.Lock()
		defer wmu.Unlock()

		var result []watched
		for _, w := range wseen {
			if len(result) == 0 || !result[len(result)-1].equals(&w) {
				result = append(result, w)
			}
		}
		return result
	}

	waitForEntries := func(entryCount int) []watched {
		var current []watched
		var expire = time.Now().Add(5 * time.Second)

		for time.Now().Before(expire) {
			current = allSeen()
			if len(current) >= entryCount {
				return current
			}
			time.Sleep(2 * time.Millisecond)
		}
		t.Fatalf("Failed to receive %d entries after 5s (got %d entries so far)", entryCount, len(current))
		return nil
	}

	rs.WatchSrvKeyspace(context.Background(), "test_cell", "test_ks", func(keyspace *topodatapb.SrvKeyspace, err error) bool {
		wmu.Lock()
		defer wmu.Unlock()
		wseen = append(wseen, watched{keyspace: keyspace, err: err})
		return true
	})

	seen1 := allSeen()
	assert.Len(t, seen1, 1)
	assert.Nil(t, seen1[0].keyspace)
	assert.True(t, topo.IsErrType(seen1[0].err, topo.NoNode))

	// Set SrvKeyspace with no values
	want := &topodatapb.SrvKeyspace{}
	err := ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)
	require.NoError(t, err)

	seen2 := waitForEntries(2)
	assert.Len(t, seen2, 2)
	assert.NotNil(t, seen2[1].keyspace)
	assert.Nil(t, seen2[1].err)
	assert.True(t, proto.Equal(want, seen2[1].keyspace))

	// Now delete the SrvKeyspace, wait until we get the error.
	err = ts.DeleteSrvKeyspace(context.Background(), "test_cell", "test_ks")
	require.NoError(t, err)

	seen3 := waitForEntries(3)
	assert.Len(t, seen3, 3)
	assert.Nil(t, seen3[2].keyspace)
	assert.True(t, topo.IsErrType(seen3[2].err, topo.NoNode))

	keyRange, err := key.ParseShardingSpec("-")
	if err != nil || len(keyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(keyRange))
	}

	for i := 0; i < 5; i++ {
		want = &topodatapb.SrvKeyspace{
			Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
				{
					ServedType: topodatapb.TabletType_PRIMARY,
					ShardReferences: []*topodatapb.ShardReference{
						{
							// This may not be a valid shard spec, but is fine for unit test purposes
							Name:     fmt.Sprintf("%d", i),
							KeyRange: keyRange[0],
						},
					},
				},
			},
		}
		err = ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
	}

	seen4 := waitForEntries(8)
	assert.Len(t, seen4, 8)

	for i := 0; i < 5; i++ {
		w := seen4[3+i]
		assert.Nil(t, w.err)
	}

	// Now simulate a topo service error
	forceErr := topo.NewError(topo.Timeout, "test topo error")
	factory.SetError(forceErr)

	seen5 := waitForEntries(9)
	assert.Len(t, seen5, 9)
	assert.Nil(t, seen5[8].keyspace)
	assert.True(t, topo.IsErrType(seen5[8].err, topo.Timeout))

	factory.SetError(nil)

	seen6 := waitForEntries(10)
	assert.Len(t, seen6, 10)
	assert.Nil(t, seen6[9].err)
	assert.NotNil(t, seen6[9].keyspace)
}

func TestSrvKeyspaceListener(t *testing.T) {
	ts, _ := memorytopo.NewServerAndFactory("test_cell")
	srvTopoCacheTTL = 100 * time.Millisecond
	srvTopoCacheRefresh = 40 * time.Millisecond
	defer func() {
		srvTopoCacheTTL = 1 * time.Second
		srvTopoCacheRefresh = 1 * time.Second
	}()

	rs := NewResilientServer(ts, "TestGetSrvKeyspaceWatcher")

	ctx, cancel := context.WithCancel(context.Background())
	var callbackCount sync2.AtomicInt32

	// adding listener will perform callback.
	rs.WatchSrvKeyspace(context.Background(), "test_cell", "test_ks", func(srvKs *topodatapb.SrvKeyspace, err error) bool {
		callbackCount.Add(1)
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	})

	// First update (callback - 2)
	want := &topodatapb.SrvKeyspace{}
	err := ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)
	require.NoError(t, err)

	// Next callback to remove from listener
	cancel()

	// multi updates thereafter
	for i := 0; i < 5; i++ {
		want = &topodatapb.SrvKeyspace{}
		err = ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
	}

	// only 3 times the callback called for the listener
	assert.EqualValues(t, 3, callbackCount.Get())
}
