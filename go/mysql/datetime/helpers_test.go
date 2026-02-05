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

package datetime

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSizeFromString(t *testing.T) {
	testcases := []struct {
		value        string
		sizeExpected int32
	}{
		{
			value:        "2020-01-01 00:00:00",
			sizeExpected: 0,
		},
		{
			value:        "2020-01-01 00:00:00.1",
			sizeExpected: 1,
		},
		{
			value:        "2020-01-01 00:00:00.12",
			sizeExpected: 2,
		},
		{
			value:        "2020-01-01 00:00:00.123",
			sizeExpected: 3,
		},
		{
			value:        "2020-01-01 00:00:00.123456",
			sizeExpected: 6,
		},
		{
			value:        "00:00:00",
			sizeExpected: 0,
		},
		{
			value:        "00:00:00.1",
			sizeExpected: 1,
		},
		{
			value:        "00:00:00.12",
			sizeExpected: 2,
		},
		{
			value:        "00:00:00.123",
			sizeExpected: 3,
		},
		{
			value:        "00:00:00.123456",
			sizeExpected: 6,
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.value, func(t *testing.T) {
			siz := SizeFromString(testcase.value)
			assert.EqualValues(t, testcase.sizeExpected, siz)
		})
	}
}
