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

package json2

import (
	"testing"
)

func TestUnmarshal(t *testing.T) {
	tcases := []struct {
		in, err string
	}{{
		in: `{
  "l2": "val",
  "l3": [
    "l4",
    "l5"asdas"
  ]
}`,
		err: "line: 5, position 9: invalid character 'a' after array element",
	}, {
		in:  "{}",
		err: "",
	}}
	for _, tcase := range tcases {
		out := make(map[string]any)
		err := Unmarshal([]byte(tcase.in), &out)
		got := ""
		if err != nil {
			got = err.Error()
		}
		if got != tcase.err {
			t.Errorf("Unmarshal(%v) err: %v, want %v", tcase.in, got, tcase.err)
		}
	}
}
