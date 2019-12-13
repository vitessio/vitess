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
)

func TestCounterDuration(t *testing.T) {
	var gotname string
	var gotv *CounterDuration
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*CounterDuration)
	})
	v := NewCounterDuration("CounterDuration", "help")
	if gotname != "CounterDuration" {
		t.Errorf("want CounterDuration, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
	if v.Get() != 0 {
		t.Errorf("want 0, got %v", v.Get())
	}
	v.Add(time.Duration(1))
	if v.Get() != 1 {
		t.Errorf("want 1, got %v", v.Get())
	}
	if v.String() != "1" {
		t.Errorf("want 1, got %v", v.Get())
	}
}

func TestCounterDurationFunc(t *testing.T) {
	var gotname string
	var gotv *CounterDurationFunc
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*CounterDurationFunc)
	})

	v := NewCounterDurationFunc("CounterDurationFunc", "help", func() time.Duration {
		return time.Duration(1)
	})
	if gotname != "CounterDurationFunc" {
		t.Errorf("want CounterDurationFunc, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
	if v.String() != "1" {
		t.Errorf("want 1, got %v", v.String())
	}
}

func TestGaugeDuration(t *testing.T) {
	var gotname string
	var gotv *GaugeDuration
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*GaugeDuration)
	})
	v := NewGaugeDuration("GaugeDuration", "help")
	if gotname != "GaugeDuration" {
		t.Errorf("want GaugeDuration, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
	v.Set(time.Duration(5))
	if v.Get() != 5 {
		t.Errorf("want 5, got %v", v.Get())
	}
	v.Add(time.Duration(1))
	if v.Get() != 6 {
		t.Errorf("want 6, got %v", v.Get())
	}
	if v.String() != "6" {
		t.Errorf("want 6, got %v", v.Get())
	}
}

func TestGaugeDurationFunc(t *testing.T) {
	var gotname string
	var gotv *GaugeDurationFunc
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*GaugeDurationFunc)
	})

	v := NewGaugeDurationFunc("GaugeDurationFunc", "help", func() time.Duration {
		return time.Duration(1)
	})

	if gotname != "GaugeDurationFunc" {
		t.Errorf("want GaugeDurationFunc, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
	if v.String() != "1" {
		t.Errorf("want 1, got %v", v.String())
	}
}
