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

package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusReplicationRunning(t *testing.T) {
	input := &ReplicationStatus{
		IOState:  ReplicationStatusToState("yes"),
		SQLState: ReplicationStatusToState("yes"),
	}
	want := true
	if got := input.Running(); got != want {
		t.Errorf("%#v.Running() = %v, want %v", input, got, want)
	}
}

func TestStatusIOThreadNotRunning(t *testing.T) {
	input := &ReplicationStatus{
		IOState:  ReplicationStatusToState("no"),
		SQLState: ReplicationStatusToState("yes"),
	}
	want := false
	if got := input.Running(); got != want {
		t.Errorf("%#v.Running() = %v, want %v", input, got, want)
	}
}

func TestStatusSQLThreadNotRunning(t *testing.T) {
	input := &ReplicationStatus{
		IOState:  ReplicationStatusToState("yes"),
		SQLState: ReplicationStatusToState("no"),
	}
	want := false
	if got := input.Running(); got != want {
		t.Errorf("%#v.Running() = %v, want %v", input, got, want)
	}
}

func TestFindErrantGTIDs(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
	sid3 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17}
	sid4 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18}
	sourceSID := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 19}

	set1 := Mysql56GTIDSet{
		sid1:      []interval{{20, 30}, {35, 39}, {40, 53}, {55, 75}},
		sid2:      []interval{{1, 7}, {20, 50}, {60, 70}},
		sid4:      []interval{{1, 30}},
		sourceSID: []interval{{1, 7}, {20, 30}},
	}

	set2 := Mysql56GTIDSet{
		sid1:      []interval{{20, 30}, {35, 37}, {50, 60}},
		sid2:      []interval{{3, 5}, {22, 25}, {32, 37}, {67, 70}},
		sid3:      []interval{{1, 45}},
		sourceSID: []interval{{2, 6}, {15, 40}},
	}

	set3 := Mysql56GTIDSet{
		sid1:      []interval{{20, 30}, {35, 38}, {50, 70}},
		sid2:      []interval{{3, 5}, {22, 25}, {32, 37}, {67, 70}},
		sid3:      []interval{{1, 45}},
		sourceSID: []interval{{2, 6}, {15, 45}},
	}

	testcases := []struct {
		mainRepStatus    *ReplicationStatus
		otherRepStatuses []*ReplicationStatus
		want             Mysql56GTIDSet
	}{{
		mainRepStatus: &ReplicationStatus{SourceUUID: sourceSID, RelayLogPosition: Position{GTIDSet: set1}},
		otherRepStatuses: []*ReplicationStatus{
			{SourceUUID: sourceSID, RelayLogPosition: Position{GTIDSet: set2}},
			{SourceUUID: sourceSID, RelayLogPosition: Position{GTIDSet: set3}},
		},
		want: Mysql56GTIDSet{
			sid1: []interval{{39, 39}, {40, 49}, {71, 75}},
			sid2: []interval{{1, 2}, {6, 7}, {20, 21}, {26, 31}, {38, 50}, {60, 66}},
			sid4: []interval{{1, 30}},
		},
	}, {
		mainRepStatus:    &ReplicationStatus{SourceUUID: sourceSID, RelayLogPosition: Position{GTIDSet: set1}},
		otherRepStatuses: []*ReplicationStatus{{SourceUUID: sid1, RelayLogPosition: Position{GTIDSet: set1}}},
		// servers with the same GTID sets should not be diagnosed with errant GTIDs
		want: nil,
	}}

	for _, testcase := range testcases {
		t.Run("", func(t *testing.T) {
			got, err := testcase.mainRepStatus.FindErrantGTIDs(testcase.otherRepStatuses)
			require.NoError(t, err)
			require.Equal(t, testcase.want, got)
		})
	}
}
