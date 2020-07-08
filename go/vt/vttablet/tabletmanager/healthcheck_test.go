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

package tabletmanager

import (
	"errors"
	"fmt"
	"html/template"
	"reflect"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	// needed so that grpc client is registered
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

func TestHealthRecordDeduplication(t *testing.T) {
	now := time.Now()
	later := now.Add(5 * time.Minute)
	cases := []struct {
		left, right *HealthRecord
		duplicate   bool
	}{
		{
			left:      &HealthRecord{Time: now},
			right:     &HealthRecord{Time: later},
			duplicate: true,
		},
		{
			left:      &HealthRecord{Time: now, Error: errors.New("foo")},
			right:     &HealthRecord{Time: now, Error: errors.New("foo")},
			duplicate: true,
		},
		{
			left:      &HealthRecord{Time: now, ReplicationDelay: degradedThreshold / 2},
			right:     &HealthRecord{Time: later, ReplicationDelay: degradedThreshold / 3},
			duplicate: true,
		},
		{
			left:      &HealthRecord{Time: now, ReplicationDelay: degradedThreshold / 2},
			right:     &HealthRecord{Time: later, ReplicationDelay: degradedThreshold * 2},
			duplicate: false,
		},
		{
			left:      &HealthRecord{Time: now, Error: errors.New("foo"), ReplicationDelay: degradedThreshold * 2},
			right:     &HealthRecord{Time: later, ReplicationDelay: degradedThreshold * 2},
			duplicate: false,
		},
	}

	for _, c := range cases {
		if got := c.left.IsDuplicate(c.right); got != c.duplicate {
			t.Errorf("IsDuplicate %v and %v: got %v, want %v", c.left, c.right, got, c.duplicate)
		}
	}
}

func TestHealthRecordClass(t *testing.T) {
	cases := []struct {
		r     *HealthRecord
		state string
	}{
		{
			r:     &HealthRecord{},
			state: "healthy",
		},
		{
			r:     &HealthRecord{Error: errors.New("foo")},
			state: "unhealthy",
		},
		{
			r:     &HealthRecord{ReplicationDelay: degradedThreshold * 2},
			state: "unhappy",
		},
		{
			r:     &HealthRecord{ReplicationDelay: degradedThreshold / 2},
			state: "healthy",
		},
	}

	for _, c := range cases {
		if got := c.r.Class(); got != c.state {
			t.Errorf("class of %v: got %v, want %v", c.r, got, c.state)
		}
	}
}

var tabletAlias = &topodatapb.TabletAlias{Cell: "cell1", Uid: 42}

// fakeHealthCheck implements health.Reporter interface
type fakeHealthCheck struct {
	reportReplicationDelay time.Duration
	reportError            error
}

func (fhc *fakeHealthCheck) Report(isReplicaType, shouldQueryServiceBeRunning bool) (replicationDelay time.Duration, err error) {
	return fhc.reportReplicationDelay, fhc.reportError
}

func (fhc *fakeHealthCheck) HTMLName() template.HTML {
	return template.HTML("fakeHealthCheck")
}

// expectBroadcastData checks that runHealthCheck() broadcasted the expected
// stats (going the value for secondsBehindMaster).
func expectBroadcastData(qsc tabletserver.Controller, serving bool, healthError string, secondsBehindMaster uint32) (*tabletservermock.BroadcastData, error) {
	bd := <-qsc.(*tabletservermock.Controller).BroadcastData
	if got := bd.Serving; got != serving {
		return nil, fmt.Errorf("unexpected BroadcastData.Serving, got: %v want: %v with bd: %+v", got, serving, bd)
	}
	if got := bd.RealtimeStats.HealthError; got != healthError {
		return nil, fmt.Errorf("unexpected BroadcastData.HealthError, got: %v want: %v with bd: %+v", got, healthError, bd)
	}
	if got := bd.RealtimeStats.SecondsBehindMaster; got != secondsBehindMaster {
		return nil, fmt.Errorf("unexpected BroadcastData.SecondsBehindMaster, got: %v want: %v with bd: %+v", got, secondsBehindMaster, bd)
	}
	return bd, nil
}

// expectBroadcastDataEmpty closes the health broadcast channel and verifies
// that all broadcasted messages were consumed by expectBroadcastData().
func expectBroadcastDataEmpty(qsc tabletserver.Controller) error {
	c := qsc.(*tabletservermock.Controller).BroadcastData
	close(c)
	bd, ok := <-c
	if ok {
		return fmt.Errorf("BroadcastData channel should have been consumed, but was not: %v", bd)
	}
	return nil
}

// expectStateChange verifies that the test changed the QueryService state
// to the expected state (serving or not, specific tablet type).
func expectStateChange(qsc tabletserver.Controller, serving bool, tabletType topodatapb.TabletType) error {
	want := &tabletservermock.StateChange{
		Serving:    serving,
		TabletType: tabletType,
	}
	got := <-qsc.(*tabletservermock.Controller).StateChanges
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("unexpected state change. got: %v want: %v", got, want)
	}
	return nil
}

// expectStateChangesEmpty closes the StateChange channel and verifies
// that all sent state changes were consumed by expectStateChange().
func expectStateChangesEmpty(qsc tabletserver.Controller) error {
	c := qsc.(*tabletservermock.Controller).StateChanges
	close(c)
	sc, ok := <-c
	if ok {
		return fmt.Errorf("StateChanges channel should have been consumed, but was not: %v", sc)
	}
	return nil
}
