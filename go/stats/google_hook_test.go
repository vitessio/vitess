package stats

import (
	"testing"
	"time"
)

func TestSetHookHistogram(t *testing.T) {
	var x int64
	v := NewHistogram("", []int64{1, 2, 3})
	v.SetHook(func(n int64) { x = n })
	n := int64(1234)
	v.Add(n)
	want, got := n, x
	if want != got {
		t.Fatalf("got %d, want %d", got, want)
	}
}

func TestSetHookTimings(t *testing.T) {
	var x time.Duration
	v := NewTimings("", "cat1")
	v.SetHook(func(name string, d time.Duration) { x = d })
	d := 5 * time.Second
	v.Add("cat1", d)
	want, got := d, x
	if want != got {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSetHookMultiTimings(t *testing.T) {
	var x time.Duration
	v := NewMultiTimings("", []string{"label1", "label2"})
	v.SetHook(func(name string, d time.Duration) { x = d })
	d := 5 * time.Second
	v.Add([]string{"val1", "val2"}, d)
	want, got := d, x
	if want != got {
		t.Fatalf("got %v, want %v", got, want)
	}
}
