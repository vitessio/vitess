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
)

func TestCounter(t *testing.T) {
	var gotname string
	var gotv *Counter
	clear()
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
	clear()
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
