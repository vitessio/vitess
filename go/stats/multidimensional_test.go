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
