/*
Copyright 2020 The Vitess Authors.

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

	"github.com/stretchr/testify/assert"
)

func TestDumuxResourceExhaustedErrors(t *testing.T) {
	type testCase struct {
		msg  string
		want int
	}

	cases := []testCase{
		{"misc", ERTooManyUserConnections},
		{"grpc: received message larger than max (99282 vs. 1234): trailer", ERNetPacketTooLarge},
		{"grpc: received message larger than max (1234 vs. 1234)", ERNetPacketTooLarge},
		{"header: grpc: received message larger than max (1234 vs. 1234)", ERNetPacketTooLarge},
		// This should be explicitly handled by returning ERNetPacketTooLarge from the execturo directly
		// and therefore shouldn't need to be teased out of another error.
		{"in-memory row count exceeded allowed limit of 13", ERTooManyUserConnections},
	}

	for _, c := range cases {
		got := demuxResourceExhaustedErrors(c.msg)
		assert.Equalf(t, c.want, got, c.msg)
	}
}
