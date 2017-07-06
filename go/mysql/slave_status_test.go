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

package mysql

import "testing"

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
