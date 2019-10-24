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

package sqltypes

import (
	"testing"

	"github.com/golang/protobuf/proto"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestEventTokenMinimum(t *testing.T) {
	testcases := []struct {
		ev1      *querypb.EventToken
		ev2      *querypb.EventToken
		expected *querypb.EventToken
	}{{
		ev1:      nil,
		ev2:      nil,
		expected: nil,
	}, {
		ev1: &querypb.EventToken{
			Timestamp: 123,
		},
		ev2:      nil,
		expected: nil,
	}, {
		ev1: nil,
		ev2: &querypb.EventToken{
			Timestamp: 123,
		},
		expected: nil,
	}, {
		ev1: &querypb.EventToken{
			Timestamp: 123,
		},
		ev2: &querypb.EventToken{
			Timestamp: 456,
		},
		expected: &querypb.EventToken{
			Timestamp: 123,
		},
	}, {
		ev1: &querypb.EventToken{
			Timestamp: 456,
		},
		ev2: &querypb.EventToken{
			Timestamp: 123,
		},
		expected: &querypb.EventToken{
			Timestamp: 123,
		},
	}}

	for _, tcase := range testcases {
		got := EventTokenMinimum(tcase.ev1, tcase.ev2)
		if tcase.expected == nil && got != nil {
			t.Errorf("expected nil result for Minimum(%v, %v) but got: %v", tcase.ev1, tcase.ev2, got)
			continue
		}
		if !proto.Equal(got, tcase.expected) {
			t.Errorf("got %v but expected %v for Minimum(%v, %v)", got, tcase.expected, tcase.ev1, tcase.ev2)
		}
	}
}
