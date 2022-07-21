/*
Copyright 2021 The Vitess Authors.

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

package testutil

import (
	"encoding/json"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/test/utils"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// AssertEmergencyReparentShardResponsesEqual asserts that two
// vtctldatapb.EmergencyReparentShardResponse objects are equal, ignoring their
// respective Events field in the comparison.
func AssertEmergencyReparentShardResponsesEqual(t *testing.T, expected *vtctldatapb.EmergencyReparentShardResponse, actual *vtctldatapb.EmergencyReparentShardResponse, msgAndArgs ...any) {
	t.Helper()

	expected = proto.Clone(expected).(*vtctldatapb.EmergencyReparentShardResponse)
	expected.Events = nil

	actual = proto.Clone(actual).(*vtctldatapb.EmergencyReparentShardResponse)
	actual.Events = nil

	utils.MustMatch(t, expected, actual)
}

// AssertLogutilEventsMatch asserts that two slices of Events match, by their
// .Value fields. In the expected slice, .Value is treated as a regular
// expression; that is, it is passed as a regexp-like string to assert.Regexp.
//
// NOTE: To match events independent of ordering, callers should run both their
// expected and actual event slices through the EventValueSorter before calling
// this function. This will mutate the slices, so make a copy first if that is
// an issue for your use case.
func AssertLogutilEventsMatch(t testing.TB, expected []*logutilpb.Event, actual []*logutilpb.Event) {
	t.Helper()

	f := func(e *logutilpb.Event) *logutilpb.Event {
		return &logutilpb.Event{
			Value: e.Value,
		}
	}
	expected = clearEvents(expected, f)
	actual = clearEvents(actual, f)

	expectedBytes, err := json.Marshal(expected)
	if !assert.NoError(t, err, "could not marshal expected events as json, assertion messages will be impacted") {
		expectedBytes = nil
	}

	actualBytes, err := json.Marshal(actual)
	if !assert.NoError(t, err, "could not marshal actual events as json, assertion messages will be impacted") {
		actualBytes = nil
	}

	if !assert.Equal(t, len(expected), len(actual), "differing number of events; expected %d, have %d\nexpected bytes: %s\nactual bytes: %s\n", len(expected), len(actual), expectedBytes, actualBytes) {
		return
	}

	for i, expectedEvent := range expected {
		actualEvent := actual[i]
		assert.Regexp(t, expectedEvent.Value, actualEvent.Value, "event %d mismatch", i)
	}
}

func clearEvents(events []*logutilpb.Event, f func(*logutilpb.Event) *logutilpb.Event) []*logutilpb.Event {
	if events == nil {
		return nil
	}

	result := make([]*logutilpb.Event, len(events))
	for i, event := range events {
		result[i] = f(event)
	}

	return result
}

// AssertPlannedReparentShardResponsesEqual asserts that two
// vtctldatapb.PlannedReparentShardResponse objects are equal, ignoring their
// respective Events field in the comparison.
func AssertPlannedReparentShardResponsesEqual(t *testing.T, expected *vtctldatapb.PlannedReparentShardResponse, actual *vtctldatapb.PlannedReparentShardResponse) {
	t.Helper()

	expected = proto.Clone(expected).(*vtctldatapb.PlannedReparentShardResponse)
	expected.Events = nil

	actual = proto.Clone(actual).(*vtctldatapb.PlannedReparentShardResponse)
	actual.Events = nil

	utils.MustMatch(t, expected, actual)
}

func AssertSameTablets(t *testing.T, expected, actual []*topodatapb.Tablet) {
	sort.Slice(expected, func(i, j int) bool {
		return fmt.Sprintf("%v", expected[i]) < fmt.Sprintf("%v", expected[j])
	})
	sort.Slice(actual, func(i, j int) bool {
		return fmt.Sprintf("%v", actual[i]) < fmt.Sprintf("%v", actual[j])
	})
	utils.MustMatch(t, expected, actual)
}

// AssertKeyspacesEqual is a convenience function to assert that two
// vtctldatapb.Keyspace objects are equal, after clearing out any reserved
// proto XXX_ fields.
func AssertKeyspacesEqual(t *testing.T, expected *vtctldatapb.Keyspace, actual *vtctldatapb.Keyspace, msgAndArgs ...any) {
	t.Helper()
	utils.MustMatch(t, expected, actual)
}

// AssertLogutilEventsOccurred asserts that for something containing a slice of
// logutilpb.Event, that the container is non-nil, and the event slice is
// non-zero.
//
// This test function is generalized with an anonymous interface that any
// protobuf type containing a slice of logutilpb.Event elements called Events,
// which is the convention in protobuf types in the Vitess codebase, already
// implements.
func AssertLogutilEventsOccurred(t *testing.T, container interface{ GetEvents() []*logutilpb.Event }, msgAndArgs ...any) {
	t.Helper()

	if container == nil {
		assert.Fail(t, "Events container must not be nil", msgAndArgs...)

		return
	}

	assert.Greater(t, len(container.GetEvents()), 0, msgAndArgs...)
}

// AssertNoLogutilEventsOccurred asserts that for something containing a slice
// of logutilpb.Event, that the container is either nil, or that the event slice
// is exactly zero length.
//
// This test function is generalized with an anonymous interface that any
// protobuf type containing a slice of logutilpb.Event elements called Events,
// which is the convention in protobuf types in the Vitess codebase, already
// implements.
func AssertNoLogutilEventsOccurred(t *testing.T, container interface{ GetEvents() []*logutilpb.Event }, msgAndArgs ...any) {
	t.Helper()

	if container == nil {
		return
	}

	assert.Equal(t, len(container.GetEvents()), 0, msgAndArgs...)
}

// EventValueSorter implements sort.Interface for slices of logutil.Event,
// ordering by .Value lexicographically.
type EventValueSorter []*logutilpb.Event

func (events EventValueSorter) Len() int           { return len(events) }
func (events EventValueSorter) Swap(i, j int)      { events[i], events[j] = events[j], events[i] }
func (events EventValueSorter) Less(i, j int) bool { return events[i].Value < events[j].Value }
