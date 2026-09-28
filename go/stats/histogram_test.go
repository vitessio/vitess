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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHistogram(t *testing.T) {
	clearStats()
	h := NewHistogram("hist1", "help", []int64{1, 5})
	for i := range 10 {
		h.Add(int64(i))
	}

	assert.Equal(t, `{"1": 2, "5": 4, "inf": 4, "Count": 10, "Total": 45}`, h.String())

	counts := h.Counts()
	counts["Count"] = h.Count()
	counts["Total"] = h.Total()
	for key, want := range map[string]int64{
		"1":     2,
		"5":     4,
		"inf":   4,
		"Count": 10,
		"Total": 45,
	} {
		assert.Equal(t, want, counts[key])
	}

	assert.Equal(t, "Count", h.CountLabel())
	assert.Equal(t, "Total", h.TotalLabel())
	assert.Equal(t, []string{"1", "5", "inf"}, h.Labels())
	assert.Equal(t, []int64{1, 5}, h.Cutoffs())
	assert.Equal(t, []int64{2, 4, 4}, h.Buckets())
	assert.Equal(t, "help", h.Help())
}

func TestGenericHistogram(t *testing.T) {
	clearStats()
	h := NewGenericHistogram(
		"histgen",
		"help",
		[]int64{1, 5},
		[]string{"one", "five", "max"},
		"count",
		"total",
	)
	assert.Equal(t, `{"one": 0, "five": 0, "max": 0, "count": 0, "total": 0}`, h.String())
}

func TestInvalidGenericHistogram(t *testing.T) {
	// Use a deferred function to capture the panic that the code should throw
	defer func() {
		r := recover()
		assert.NotNil(t, r)
		assert.Equal(t, "mismatched cutoff and label lengths", r)
	}()

	clearStats()
	NewGenericHistogram(
		"histgen",
		"help",
		[]int64{1, 5},
		[]string{"one", "five"},
		"count",
		"total",
	)
}

func TestHistogramHook(t *testing.T) {
	// Check the results of Register hook function
	var gotName string
	var gotV *Histogram
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotName = name
		gotV = v.(*Histogram)
	})

	v := NewHistogram("hist2", "help", []int64{1})

	assert.Equal(t, "hist2", gotName)
	assert.Equal(t, gotV, v)

	// Check the results of AddHook function
	hookCalled := false
	var addedValue int64

	v.AddHook(func(value int64) {
		hookCalled = true
		addedValue = value
	})

	v.Add(42)
	assert.True(t, hookCalled)
	assert.Equal(t, int64(42), addedValue)

	// Check the results of RegisterHistogramHook function
	hookCalled = false
	addedValue = 0
	gotName = ""

	RegisterHistogramHook(func(name string, value int64) {
		hookCalled = true
		gotName = name
		addedValue = value
	})

	v.Add(10)
	assert.Equal(t, "hist2", gotName)
	assert.True(t, hookCalled)
	assert.Equal(t, int64(10), addedValue)
}
