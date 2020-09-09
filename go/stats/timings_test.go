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

package stats

import (
	"expvar"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimings(t *testing.T) {
	clear()
	tm := NewTimings("timings1", "help", "category")
	tm.Add("tag1", 500*time.Microsecond)
	tm.Add("tag1", 1*time.Millisecond)
	tm.Add("tag2", 1*time.Millisecond)
	want := `{"TotalCount":3,"TotalTime":2500000,"Histograms":{"tag1":{"500000":1,"1000000":1,"5000000":0,"10000000":0,"50000000":0,"100000000":0,"500000000":0,"1000000000":0,"5000000000":0,"10000000000":0,"inf":0,"Count":2,"Time":1500000},"tag2":{"500000":0,"1000000":1,"5000000":0,"10000000":0,"50000000":0,"100000000":0,"500000000":0,"1000000000":0,"5000000000":0,"10000000000":0,"inf":0,"Count":1,"Time":1000000}}}`
	if got := tm.String(); got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestMultiTimings(t *testing.T) {
	clear()
	mtm := NewMultiTimings("maptimings1", "help", []string{"dim1", "dim2"})
	mtm.Add([]string{"tag1a", "tag1b"}, 500*time.Microsecond)
	mtm.Add([]string{"tag1a", "tag1b"}, 1*time.Millisecond)
	mtm.Add([]string{"tag2a", "tag2b"}, 1*time.Millisecond)
	want := `{"TotalCount":3,"TotalTime":2500000,"Histograms":{"tag1a.tag1b":{"500000":1,"1000000":1,"5000000":0,"10000000":0,"50000000":0,"100000000":0,"500000000":0,"1000000000":0,"5000000000":0,"10000000000":0,"inf":0,"Count":2,"Time":1500000},"tag2a.tag2b":{"500000":0,"1000000":1,"5000000":0,"10000000":0,"50000000":0,"100000000":0,"500000000":0,"1000000000":0,"5000000000":0,"10000000000":0,"inf":0,"Count":1,"Time":1000000}}}`
	if got := mtm.String(); got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestMultiTimingsDot(t *testing.T) {
	clear()
	mtm := NewMultiTimings("maptimings2", "help", []string{"label"})
	mtm.Add([]string{"value.dot"}, 500*time.Microsecond)
	safe := safeLabel("value.dot")
	safeJSON := strings.Replace(safe, "\\", "\\\\", -1)
	want := `{"TotalCount":1,"TotalTime":500000,"Histograms":{"` + safeJSON + `":{"500000":1,"1000000":0,"5000000":0,"10000000":0,"50000000":0,"100000000":0,"500000000":0,"1000000000":0,"5000000000":0,"10000000000":0,"inf":0,"Count":1,"Time":500000}}}`
	if got := mtm.String(); got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestTimingsHook(t *testing.T) {
	var gotname string
	var gotv *Timings
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Timings)
	})

	name := "timings2"
	v := NewTimings(name, "help", "")
	if gotname != name {
		t.Errorf("got %q, want %q", gotname, name)
	}
	if gotv != v {
		t.Errorf("got %#v, want %#v", gotv, v)
	}
}

func TestTimingsCombineDimension(t *testing.T) {
	clear()
	*combineDimensions = "a,c"

	t1 := NewTimings("timing_combine_dim1", "help", "label")
	t1.Add("t1", 1*time.Nanosecond)
	want := `{"TotalCount":1,"TotalTime":1,"Histograms":{"t1":{"500000":1,"1000000":0,"5000000":0,"10000000":0,"50000000":0,"100000000":0,"500000000":0,"1000000000":0,"5000000000":0,"10000000000":0,"inf":0,"Count":1,"Time":1}}}`
	assert.Equal(t, want, t1.String())

	t2 := NewTimings("timing_combine_dim2", "help", "a")
	t2.Add("t1", 1)
	want = `{"TotalCount":1,"TotalTime":1,"Histograms":{"all":{"500000":1,"1000000":0,"5000000":0,"10000000":0,"50000000":0,"100000000":0,"500000000":0,"1000000000":0,"5000000000":0,"10000000000":0,"inf":0,"Count":1,"Time":1}}}`
	assert.Equal(t, want, t2.String())

	t3 := NewMultiTimings("timing_combine_dim3", "help", []string{"a", "b", "c"})
	t3.Add([]string{"c1", "c2", "c3"}, 1)
	want = `{"TotalCount":1,"TotalTime":1,"Histograms":{"all.c2.all":{"500000":1,"1000000":0,"5000000":0,"10000000":0,"50000000":0,"100000000":0,"500000000":0,"1000000000":0,"5000000000":0,"10000000000":0,"inf":0,"Count":1,"Time":1}}}`
	assert.Equal(t, want, t3.String())
}
