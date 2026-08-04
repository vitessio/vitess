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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCounterDuration(t *testing.T) {
	var gotname string
	var gotv *CounterDuration
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*CounterDuration)
	})
	v := NewCounterDuration("CounterDuration", "help")
	assert.Equal(t, "CounterDuration", gotname)
	assert.Same(t, v, gotv)
	assert.EqualValues(t, 0, v.Get())
	v.Add(time.Duration(1))
	assert.EqualValues(t, 1, v.Get())
	assert.Equal(t, "1", v.String())
}

func TestCounterDurationFunc(t *testing.T) {
	var gotname string
	var gotv *CounterDurationFunc
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*CounterDurationFunc)
	})

	v := NewCounterDurationFunc("CounterDurationFunc", "help", func() time.Duration {
		return time.Duration(1)
	})
	assert.Equal(t, "CounterDurationFunc", gotname)
	assert.Same(t, v, gotv)
	assert.Equal(t, "1", v.String())
}

func TestGaugeDuration(t *testing.T) {
	var gotname string
	var gotv *GaugeDuration
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*GaugeDuration)
	})
	v := NewGaugeDuration("GaugeDuration", "help")
	assert.Equal(t, "GaugeDuration", gotname)
	assert.Same(t, v, gotv)
	v.Set(time.Duration(5))
	assert.EqualValues(t, 5, v.Get())
	v.Add(time.Duration(1))
	assert.EqualValues(t, 6, v.Get())
	assert.Equal(t, "6", v.String())
}

func TestGaugeDurationFunc(t *testing.T) {
	var gotname string
	var gotv *GaugeDurationFunc
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*GaugeDurationFunc)
	})

	v := NewGaugeDurationFunc("GaugeDurationFunc", "help", func() time.Duration {
		return time.Duration(1)
	})

	assert.Equal(t, "GaugeDurationFunc", gotname)
	assert.Same(t, v, gotv)
	assert.Equal(t, "1", v.String())
}
