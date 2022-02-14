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
