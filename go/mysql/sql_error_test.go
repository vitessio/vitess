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

func TestDumuxResourceExhaustedErrors(t *testing.T) {
	type testCase struct {
		msg  string
		want int
	}

	cases := []testCase{
		testCase{"misc", ERTooManyUserConnections},
		testCase{"grpc: received message larger than max (99282+ vs. 1234): trailer", ERTooManyUserConnections},
		testCase{"grpc: received message larger than max (1234 vs. 1234)", ERNetPacketTooLarge},
		testCase{"header: grpc: received message larger than max (1234 vs. 1234)", ERNetPacketTooLarge},
	}

	for _, c := range cases {
		got := demuxResourceExhaustedErrors(c.msg)
		if got != c.want {
			t.Errorf("For error %s - got %v, wanted %v", c.msg, got, c.want)
		}
	}
}
