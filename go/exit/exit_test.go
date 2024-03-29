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

package exit

import (
	"testing"
)

type repanicType int

func TestReturn(t *testing.T) {
	defer func() {
		err := recover()
		if err == nil {
			t.Errorf("Return() did not panic with exit code")
		}

		switch code := err.(type) {
		case exitCode:
			if code != 152 {
				t.Errorf("got %v, want %v", code, 152)
			}
		default:
			panic(err)
		}
	}()

	Return(152)
}

func TestRecover(t *testing.T) {
	var code int

	exitFunc = func(c int) {
		code = c
	}

	func() {
		defer Recover()
		Return(8235)
	}()

	if code != 8235 {
		t.Errorf("got %v, want %v", code, 8235)
	}
}

func TestRecoverRepanic(t *testing.T) {
	defer func() {
		err := recover()

		if err == nil {
			t.Errorf("Recover() didn't re-panic an error other than exitCode")
			return
		}

		if _, ok := err.(repanicType); !ok {
			panic(err) // something unexpected went wrong
		}
	}()

	defer Recover()

	panic(repanicType(1))
}

func TestRecoverAll(t *testing.T) {
	exitFunc = func(int) {}

	defer func() {
		err := recover()

		if err != nil {
			t.Errorf("RecoverAll() didn't absorb all panics")
		}
	}()

	defer RecoverAll()

	panic(repanicType(1))
}

// TestRecoverNil checks that Recover() does nothing when there is no panic.
func TestRecoverNil(t *testing.T) {
	defer Recover()
}
