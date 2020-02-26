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
	"errors"
	"testing"
)

func TestIsConnErr(t *testing.T) {
	testcases := []struct {
		in   error
		want bool
	}{{
		in:   errors.New("t"),
		want: false,
	}, {
		in:   NewSQLError(5, "", ""),
		want: false,
	}, {
		in:   NewSQLError(CRServerGone, "", ""),
		want: true,
	}, {
		in:   NewSQLError(CRServerLost, "", ""),
		want: true,
	}, {
		in:   NewSQLError(ERQueryInterrupted, "", ""),
		want: true,
	}, {
		in:   NewSQLError(CRCantReadCharset, "", ""),
		want: false,
	}}
	for _, tcase := range testcases {
		got := IsConnErr(tcase.in)
		if got != tcase.want {
			t.Errorf("IsConnErr(%#v): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}
