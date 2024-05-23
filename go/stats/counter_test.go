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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCounter(t *testing.T) {
	var gotname string
	var gotv *Counter
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Counter)
	})
	v := NewCounter("Int", "help")
	if gotname != "Int" {
		t.Errorf("want Int, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
	v.Add(1)
	if v.Get() != 1 {
		t.Errorf("want 1, got %v", v.Get())
	}
	if v.String() != "1" {
		t.Errorf("want 1, got %v", v.Get())
	}
	v.Reset()
	if v.Get() != 0 {
		t.Errorf("want 0, got %v", v.Get())
	}
}

func TestGaugeFunc(t *testing.T) {
	var gotname string
	var gotv *GaugeFunc
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*GaugeFunc)
	})

	v := NewGaugeFunc("name", "help", func() int64 {
		return 1
	})
	if gotname != "name" {
		t.Errorf("want name, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
	if v.String() != "1" {
		t.Errorf("want 1, got %v", v.String())
	}
}

func TestGaugeFloat64(t *testing.T) {
	var gotname string
	var gotv *GaugeFloat64
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*GaugeFloat64)
	})
	v := NewGaugeFloat64("f", "help")
	assert.Equal(t, "f", gotname)
	assert.Equal(t, v, gotv)
	v.Set(3.14)
	assert.Equal(t, 3.14, v.Get())
	assert.Equal(t, "3.14", v.String())
	v.Reset()
	assert.Equal(t, float64(0), v.Get())
}

func TestNewCounterWithDeprecatedName(t *testing.T) {
	clearStats()
	Register(func(name string, v expvar.Var) {})

	testcases := []struct {
		name           string
		deprecatedName string
		shouldPanic    bool
	}{
		{
			name:           "new_name",
			deprecatedName: "deprecatedName",
			shouldPanic:    true,
		},
		{
			name:           "metricName_test",
			deprecatedName: "metric.name-test",
			shouldPanic:    false,
		},
		{
			name:           "MetricNameTesting",
			deprecatedName: "metric.name.testing",
			shouldPanic:    false,
		},
	}

	for _, testcase := range testcases {
		t.Run(fmt.Sprintf("%v-%v", testcase.name, testcase.deprecatedName), func(t *testing.T) {
			wg := sync.WaitGroup{}
			wg.Add(1)
			panicReceived := false
			go func() {
				defer func() {
					if x := recover(); x != nil {
						panicReceived = true
					}
					wg.Done()
				}()
				NewCounterWithDeprecatedName(testcase.name, testcase.deprecatedName, "help")
			}()
			wg.Wait()
			require.EqualValues(t, testcase.shouldPanic, panicReceived)
		})
	}
}

func TestNewGaugeWithDeprecatedName(t *testing.T) {
	clearStats()
	Register(func(name string, v expvar.Var) {})

	testcases := []struct {
		name           string
		deprecatedName string
		shouldPanic    bool
	}{
		{
			name:           "gauge_new_name",
			deprecatedName: "gauge_deprecatedName",
			shouldPanic:    true,
		},
		{
			name:           "gauge-metricName_test",
			deprecatedName: "gauge_metric.name-test",
			shouldPanic:    false,
		},
		{
			name:           "GaugeMetricNameTesting",
			deprecatedName: "gauge.metric.name.testing",
			shouldPanic:    false,
		},
	}

	for _, testcase := range testcases {
		t.Run(fmt.Sprintf("%v-%v", testcase.name, testcase.deprecatedName), func(t *testing.T) {
			wg := sync.WaitGroup{}
			wg.Add(1)
			panicReceived := false
			go func() {
				defer func() {
					if x := recover(); x != nil {
						panicReceived = true
					}
					wg.Done()
				}()
				NewGaugeWithDeprecatedName(testcase.name, testcase.deprecatedName, "help")
			}()
			wg.Wait()
			require.EqualValues(t, testcase.shouldPanic, panicReceived)
		})
	}
}
