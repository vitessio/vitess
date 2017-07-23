/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stats

import (
	"reflect"
	"testing"
	"time"
)

func TestMultiTimingsCounterFor(t *testing.T) {
	clear()
	mtm := NewMultiTimings("multitimings3", []string{"dim1", "dim2"})

	mtm.Add([]string{"tag1a", "tag1b"}, 500*time.Microsecond)
	mtm.Add([]string{"tag1a", "tag2b"}, 500*time.Millisecond)
	mtm.Add([]string{"tag2a", "tag2b"}, 500*time.Millisecond)

	cases := []struct {
		dim  string
		want map[string]int64
	}{
		{"dim1", map[string]int64{"tag1a": 2, "tag2a": 1, "All": 3}},
		{"dim2", map[string]int64{"tag1b": 1, "tag2b": 2, "All": 3}},
	}
	for _, c := range cases {
		counts := CounterForDimension(mtm, c.dim).Counts()
		if !reflect.DeepEqual(c.want, counts) {
			t.Errorf("mtm.CounterFor(%q).Counts()=%v, want %v", c.dim, counts, c.want)
		}
	}
}
