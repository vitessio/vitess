/*
Copyright 2021 The Vitess Authors.

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
	"strconv"
	"testing"
)

func TestFuzzHandleNextCommand(t *testing.T) {
	testcases := [][]byte{
		{0x20, 0x20, 0x20, 0x00, 0x16, 0x20, 0x20, 0x20, 0x20, 0x20},
	}
	for i, testcase := range testcases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			FuzzHandleNextCommand(testcase)
		})
	}
}
