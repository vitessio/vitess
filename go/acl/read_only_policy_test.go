/*
Copyright 2024 The Vitess Authors.

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

package acl

import "testing"

func TestReadOnlyPolicy(t *testing.T) {
	testReadOnlyPolicy := readOnlyPolicy{}

	want := errReadOnly
	err := testReadOnlyPolicy.CheckAccessActor("", ADMIN)
	if err == nil || err != want {
		t.Errorf("got %v; want %v", err, want)
	}

	err = testReadOnlyPolicy.CheckAccessActor("", DEBUGGING)
	if err != nil {
		t.Errorf("got %v; want no error", err)
	}

	err = testReadOnlyPolicy.CheckAccessActor("", MONITORING)
	if err != nil {
		t.Errorf("got %v; want no error", err)
	}

	err = testReadOnlyPolicy.CheckAccessHTTP(nil, ADMIN)
	if err == nil || err != want {
		t.Errorf("got %v; want %v", err, want)
	}

	err = testReadOnlyPolicy.CheckAccessHTTP(nil, DEBUGGING)
	if err != nil {
		t.Errorf("got %v; want no error", err)
	}

	err = testReadOnlyPolicy.CheckAccessHTTP(nil, MONITORING)
	if err != nil {
		t.Errorf("got %v; want no error", err)
	}
}
