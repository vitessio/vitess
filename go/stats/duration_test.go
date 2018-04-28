/*
Copyright 2018 The Vitess Authors

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

func TestDuration(t *testing.T) {
	var gotname string
	var gotv *Duration
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Duration)
	})
	v := NewDuration("Duration", "help")
	if gotname != "Duration" {
		t.Errorf("want Duration, got %s", gotname)
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

func TestDurationFunc(t *testing.T) {
	var gotname string
	var gotv *DurationFunc
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*DurationFunc)
	})

	v := NewDurationFunc("duration", "help", func() time.Duration {
		return time.Duration(1)
	})

	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
	if v.String() != "1" {
		t.Errorf("want 1, got %v", v.String())
	}
	if gotname != "duration" {
		t.Errorf("want duration, got %s", gotname)
	}
}
