package janitor

import (
	"errors"
	"github.com/youtube/vitess/go/vt/wrangler"
	"math"
	"testing"
	"time"
)

//var testCells = []string{"oe", "wj"}

type testJanitor struct {
	run       func(bool) error
	configure func(wr *wrangler.Wrangler, keyspace, shard string) error
}

func (janitor *testJanitor) Configure(wr *wrangler.Wrangler, keyspace, shard string) error {
	return janitor.configure(wr, keyspace, shard)
}

func (janitor *testJanitor) Run(active bool) error {
	return janitor.run(active)
}

func newTestJanitor() *testJanitor {
	return &testJanitor{
		run: func(bool) error { return nil },
		configure: func(wr *wrangler.Wrangler, keyspace, shard string) error {
			return nil
		},
	}
}

func aroundSameDuration(d1, d2, delta time.Duration) bool {
	return math.Abs(float64(d1-d2)) < float64(delta)
}

func TestJanitorInfo(t *testing.T) {
	jan := newJanitorInfo(nil)
	jan.RecordSuccess(time.Now().Add(-10 * time.Second))
	jan.RecordSuccess(time.Now().Add(-20 * time.Second))
	jan.RecordSuccess(time.Now().Add(-30 * time.Second))

	if got, want := jan.AverageRuntime(), 20*time.Second; !aroundSameDuration(want, got, 500*time.Millisecond) {
		t.Errorf("jan.AverageRuntime: want %v, got %v", want, got)
	}
}

func TestRunJanitor(t *testing.T) {
	scheduler, _ := New("a", "a", nil, nil, 0)
	jan := newTestJanitor()
	ji := newJanitorInfo(jan)
	scheduler.janitors["test"] = ji

	scheduler.runJanitor("test")

	if ji.Runs() != 1 {
		t.Errorf("jan.Runs: want 1, got %v", ji.Runs())
	}

	if ji.ErrorCount() != 0 {
		t.Errorf("ji.ErrorCount: want 0, got %v", ji.ErrorCount())
	}

	jan.run = func(bool) error {
		return errors.New("error")
	}

	scheduler.runJanitor("test")

	if ji.ErrorCount() != 1 {
		t.Errorf("ji.ErrorCount: want 0, got %v", ji.ErrorCount())
	}

}
