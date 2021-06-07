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
func AssertEmergencyReparentShardResponsesEqual(t *testing.T, expected *vtctldatapb.EmergencyReparentShardResponse, actual *vtctldatapb.EmergencyReparentShardResponse, msgAndArgs ...interface{}) {
	t.Helper()

	expected = proto.Clone(expected).(*vtctldatapb.EmergencyReparentShardResponse)
	expected.Events = nil

	actual = proto.Clone(actual).(*vtctldatapb.EmergencyReparentShardResponse)
	actual.Events = nil

	utils.MustMatch(t, expected, actual)
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
func AssertKeyspacesEqual(t *testing.T, expected *vtctldatapb.Keyspace, actual *vtctldatapb.Keyspace, msgAndArgs ...interface{}) {
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
func AssertLogutilEventsOccurred(t *testing.T, container interface{ GetEvents() []*logutilpb.Event }, msgAndArgs ...interface{}) {
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
func AssertNoLogutilEventsOccurred(t *testing.T, container interface{ GetEvents() []*logutilpb.Event }, msgAndArgs ...interface{}) {
	t.Helper()

	if container == nil {
		return
	}

	assert.Equal(t, len(container.GetEvents()), 0, msgAndArgs...)
}
