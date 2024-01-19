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
	for i := 0; i < 10; i++ {
		h.Add(int64(i))
	}

	assert.Equal(t, h.String(), `{"1": 2, "5": 4, "inf": 4, "Count": 10, "Total": 45}`)

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
		assert.Equal(t, counts[key], want)
	}

	assert.Equal(t, h.CountLabel(), "Count")
	assert.Equal(t, h.TotalLabel(), "Total")
	assert.Equal(t, h.Labels(), []string{"1", "5", "inf"})
	assert.Equal(t, h.Cutoffs(), []int64{1, 5})
	assert.Equal(t, h.Buckets(), []int64{2, 4, 4})
	assert.Equal(t, h.Help(), "help")
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
	assert.Equal(t, h.String(), `{"one": 0, "five": 0, "max": 0, "count": 0, "total": 0}`)
}

func TestInvalidGenericHistogram(t *testing.T) {
	// Use a deferred function to capture the panic that the code should throw
	defer func() {
		r := recover()
		assert.NotNil(t, r)
		assert.Equal(t, r, "mismatched cutoff and label lengths")
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

	assert.Equal(t, gotName, "hist2")
	assert.Equal(t, gotV, v)

	// Check the results of AddHook function
	hookCalled := false
	var addedValue int64

	v.AddHook(func(value int64) {
		hookCalled = true
		addedValue = value
	})

	v.Add(42)
	assert.Equal(t, hookCalled, true)
	assert.Equal(t, addedValue, int64(42))

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
	assert.Equal(t, gotName, "hist2")
	assert.Equal(t, hookCalled, true)
	assert.Equal(t, addedValue, int64(10))
}
