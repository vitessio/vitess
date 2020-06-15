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
)

func TestStatusSlaveRunning(t *testing.T) {
	input := &SlaveStatus{
		SlaveIORunning:  true,
		SlaveSQLRunning: true,
	}
	want := true
	if got := input.SlaveRunning(); got != want {
		t.Errorf("%#v.SlaveRunning() = %v, want %v", input, got, want)
	}
}

func TestStatusSlaveIONotRunning(t *testing.T) {
	input := &SlaveStatus{
		SlaveIORunning:  false,
		SlaveSQLRunning: true,
	}
	want := false
	if got := input.SlaveRunning(); got != want {
		t.Errorf("%#v.SlaveRunning() = %v, want %v", input, got, want)
	}
}

func TestStatusSlaveSQLNotRunning(t *testing.T) {
	input := &SlaveStatus{
		SlaveIORunning:  true,
		SlaveSQLRunning: false,
	}
	want := false
	if got := input.SlaveRunning(); got != want {
		t.Errorf("%#v.SlaveRunning() = %v, want %v", input, got, want)
	}
}

func TestFindErrantGTIDs(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
	sid3 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17}
	sid4 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18}
	masterSID := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 19}

	set1 := Mysql56GTIDSet{
		sid1:      []interval{{20, 30}, {35, 39}, {40, 53}, {55, 75}},
		sid2:      []interval{{1, 7}, {20, 50}, {60, 70}},
		sid4:      []interval{{1, 30}},
		masterSID: []interval{{1, 7}, {20, 30}},
	}

	set2 := Mysql56GTIDSet{
		sid1:      []interval{{20, 30}, {35, 37}, {50, 60}},
		sid2:      []interval{{3, 5}, {22, 25}, {32, 37}, {67, 70}},
		sid3:      []interval{{1, 45}},
		masterSID: []interval{{2, 6}, {15, 40}},
	}

	set3 := Mysql56GTIDSet{
		sid1:      []interval{{20, 30}, {35, 38}, {50, 70}},
		sid2:      []interval{{3, 5}, {22, 25}, {32, 37}, {67, 70}},
		sid3:      []interval{{1, 45}},
		masterSID: []interval{{2, 6}, {15, 45}},
	}

	slaveStatus1 := SlaveStatus{MasterUUID: masterSID, RelayLogPosition: Position{GTIDSet: set1}}
	slaveStatus2 := SlaveStatus{MasterUUID: masterSID, RelayLogPosition: Position{GTIDSet: set2}}
	slaveStatus3 := SlaveStatus{MasterUUID: masterSID, RelayLogPosition: Position{GTIDSet: set3}}

	got, err := slaveStatus1.FindErrantGTIDs([]*SlaveStatus{&slaveStatus2, &slaveStatus3})
	if err != nil {
		t.Errorf("%v", err)
	}

	want := Mysql56GTIDSet{
		sid1: []interval{{39, 39}, {40, 49}, {71, 75}},
		sid2: []interval{{1, 2}, {6, 7}, {20, 21}, {26, 31}, {38, 50}, {60, 66}},
		sid4: []interval{{1, 30}},
	}

	if !got.Equal(want) {
		t.Errorf("got %#v; want %#v", got, want)
	}
}
